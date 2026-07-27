use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Reads from `stream` until the client closes its end, writing every
/// chunk straight back. Identical to stage 1 — the mechanics of serving
/// one connection haven't changed, only how many can be in flight at once.
pub fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        stream.write_all(&buf[..n])?;
    }
}

/// The default cap on concurrent connections `run()` uses. Thread-per-
/// connection has no natural upper bound otherwise — see
/// `run_with_limit`'s doc comment for why that matters.
const MAX_CONNECTIONS: usize = 1024;

/// A counting semaphore built from std primitives only (no external
/// crate — stages 1-6 stay dependency-free by design). Caps how many
/// connections can be in flight at once: `acquire` blocks the accept
/// loop itself once the cap is reached, so `thread::spawn` is never the
/// thing that eventually fails under unbounded connection growth.
struct Semaphore {
    available: Mutex<usize>,
    condvar: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Semaphore {
            available: Mutex::new(permits),
            condvar: Condvar::new(),
        }
    }

    /// Blocks until a permit is available, then takes one. The returned
    /// guard releases it automatically on drop — including if the
    /// thread holding it panics — so a slot is never leaked.
    fn acquire(self: &Arc<Self>) -> SemaphorePermit {
        let mut available = self.available.lock().unwrap();
        while *available == 0 {
            available = self.condvar.wait(available).unwrap();
        }
        *available -= 1;
        SemaphorePermit {
            semaphore: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut available = self.available.lock().unwrap();
        *available += 1;
        // Only one waiter can make use of the permit being returned;
        // waking every waiter to have all but one immediately re-block
        // would just be wasted work.
        self.condvar.notify_one();
    }
}

struct SemaphorePermit {
    semaphore: Arc<Semaphore>,
}

impl Drop for SemaphorePermit {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// Accepts connections from `listener` forever, spawning a new OS thread
/// per connection so a slow or long-lived client never blocks any other
/// client's `accept()` — the limitation stage 1 deliberately left in.
/// Caps concurrent connections at [`MAX_CONNECTIONS`]; see
/// `run_with_limit` for why a cap exists at all.
pub fn run(listener: TcpListener) -> io::Result<()> {
    run_with_limit(listener, MAX_CONNECTIONS)
}

/// Same as `run`, but with an explicit cap on concurrent connections
/// instead of the default. Split out so the limiting behavior itself is
/// directly testable without waiting on `MAX_CONNECTIONS`-many clients.
///
/// Without a cap, every accepted connection gets its own OS thread with
/// no limit at all — each thread reserves a real stack (2MiB by default
/// on most platforms), and `thread::spawn` doesn't return a `Result`:
/// it panics if the OS refuses to create the thread (hit `ulimit -u`,
/// exhausted memory for stacks, etc.). That panic happens on the same
/// thread running this accept loop, so an unbounded pile of slow or
/// idle-forever clients (see stage 1's notes on a client that never
/// closes) doesn't just cost memory gradually — it eventually crashes
/// the whole server, taking every already-connected client down with
/// it. Capping concurrency turns that crash into ordinary backpressure:
/// once `max_connections` clients are being served, the accept loop
/// simply blocks (via the semaphore) until one finishes, so new
/// connections queue up in the OS's own accept backlog instead.
pub fn run_with_limit(listener: TcpListener, max_connections: usize) -> io::Result<()> {
    let semaphore = Arc::new(Semaphore::new(max_connections));
    for stream in listener.incoming() {
        // Scoped the same way a failed connection is, below: log it and
        // keep serving, rather than letting one bad accept take the
        // whole server down via `?` (same fix made in stage 1).
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                // See stage 1's matching comment: a persistently failing
                // accept() would otherwise busy-spin this loop.
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let permit = semaphore.acquire();
        thread::spawn(move || {
            let _permit = permit; // held for the connection's whole lifetime
            if let Err(e) = handle_connection(stream) {
                eprintln!("connection error: {e}");
            }
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn semaphore_blocks_a_second_acquire_until_the_first_permit_is_dropped() {
        let semaphore = Arc::new(Semaphore::new(1));
        let first_permit = semaphore.acquire();

        let second = Arc::clone(&semaphore);
        let (unblocked_tx, unblocked_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let _second_permit = second.acquire();
            unblocked_tx.send(()).unwrap();
        });

        // The waiter should still be blocked in acquire() — give it a
        // moment to prove that, rather than just racing it.
        thread::sleep(Duration::from_millis(50));
        assert!(
            unblocked_rx.try_recv().is_err(),
            "acquire() returned before any permit was available"
        );

        drop(first_permit);
        unblocked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("acquire() should unblock once a permit is released");
        waiter.join().unwrap();
    }

    fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    #[test]
    fn echoes_a_single_write() {
        let (mut client, server) = connected_pair();
        let handle = thread::spawn(move || handle_connection(server));

        client.write_all(b"hello").unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");

        drop(client);
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn echoes_multiple_writes_on_one_connection() {
        let (mut client, server) = connected_pair();
        let handle = thread::spawn(move || handle_connection(server));

        for chunk in [&b"one"[..], &b"two"[..], &b"three"[..]] {
            client.write_all(chunk).unwrap();
            let mut buf = vec![0u8; chunk.len()];
            client.read_exact(&mut buf).unwrap();
            assert_eq!(buf, chunk);
        }

        drop(client);
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn echoes_a_payload_larger_than_the_internal_chunk_size() {
        let (mut client, server) = connected_pair();
        let handle = thread::spawn(move || handle_connection(server));

        let payload = vec![0x42u8; 10_000]; // bigger than the 4096-byte buf
        let mut reader = client.try_clone().unwrap();
        let reader_thread = thread::spawn(move || {
            let mut received = vec![0u8; 10_000];
            reader.read_exact(&mut received).unwrap();
            received
        });

        client.write_all(&payload).unwrap();
        let received = reader_thread.join().unwrap();
        assert_eq!(received, payload);

        drop(client);
        handle.join().unwrap().unwrap();
    }
}
