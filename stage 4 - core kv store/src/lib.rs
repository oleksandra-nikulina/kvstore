pub mod command;
pub mod resp;
pub mod store;

use command::{ReadResult, execute, read_command};
use resp::Reply;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use store::Store;

/// Reads RESP commands off `stream`, executes each one against the
/// shared `store`, and writes back its reply — until the client
/// disconnects or sends bytes that don't parse as RESP.
///
/// One flat loop, not two nested ones: there's exactly one thing to
/// wait on (more bytes from the socket), so "process everything already
/// buffered" and "go get more" fold into one match arm. `pos` tracks how
/// much of `buf` has already been parsed and replied to without
/// physically removing it yet — compacting once per socket read (right
/// before blocking for more) instead of once per command keeps draining
/// linear in the number of bytes processed, rather than quadratic in a
/// heavily pipelined batch's size.
pub fn handle_connection(mut stream: TcpStream, store: &Store) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 4096];
    let mut pos = 0;

    loop {
        match read_command(&buf[pos..]) {
            Ok(ReadResult::Incomplete) => {
                if pos > 0 {
                    buf.drain(0..pos);
                    pos = 0;
                }
                let n = stream.read(&mut read_buf)?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&read_buf[..n]);
            }
            Ok(ReadResult::Empty { consumed }) => {
                pos += consumed;
            }
            Ok(ReadResult::Command { command, consumed }) => {
                let reply = execute(&command, store);
                stream.write_all(&reply.encode())?;
                pos += consumed;
            }
            Err(e) => {
                let reply = Reply::Error(format!("ERR Protocol error: {e}"));
                stream.write_all(&reply.encode())?;
                return Ok(());
            }
        }
    }
}

/// The default cap on concurrent connections `run()` uses — same
/// reasoning as stages 2-3, carried forward here since each stage is an
/// independent crate.
const MAX_CONNECTIONS: usize = 1024;

/// A counting semaphore built from std primitives only (no external
/// crate — stages 1-6 stay dependency-free by design), identical to
/// stages 2-3's.
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

    fn acquire(self: &Arc<Self>) -> SemaphorePermit {
        // `.unwrap_or_else(..into_inner())` here, not a bare `.unwrap()`,
        // for the same reason as `Store`'s `recover()`: this `Mutex`
        // guards nothing but an integer counter, so poisoning can't
        // realistically happen — but if it ever did, cascading that as
        // a fresh panic would deny every other connection a permit too.
        let mut available = self
            .available
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *available == 0 {
            available = self
                .condvar
                .wait(available)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *available -= 1;
        SemaphorePermit {
            semaphore: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut available = self
            .available
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *available += 1;
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

/// Accepts connections forever, one thread per connection, all sharing
/// one `Store` behind an `Arc` — this is where stage 2's concurrency
/// model and stage 3's protocol meet real shared mutable state. Caps
/// concurrent connections at [`MAX_CONNECTIONS`]; see stage 2's
/// `run_with_limit` notes for why a cap exists at all.
pub fn run(listener: TcpListener) -> io::Result<()> {
    let store = Arc::new(Store::new());
    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    for stream in listener.incoming() {
        // Scoped the same way a failed connection is, below: log it and
        // keep serving, rather than letting one bad accept take the
        // whole server down via `?`.
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                // A persistently failing accept() would otherwise
                // busy-spin this loop.
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let store = Arc::clone(&store);
        let permit = semaphore.acquire();
        thread::spawn(move || {
            let _permit = permit; // held for the connection's whole lifetime
            if let Err(e) = handle_connection(stream, &store) {
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
}
