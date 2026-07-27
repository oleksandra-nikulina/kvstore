pub mod command;
pub mod resp;

use command::{ReadResult, execute, read_command};
use resp::Reply;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Reads RESP commands off `stream`, executes each one, and writes back
/// its reply — until the client disconnects or sends bytes that don't
/// parse as RESP, at which point an error reply is sent and the
/// connection is closed (the framing is unrecoverable at that point:
/// there's no way to know where the next command would start).
///
/// One flat loop is enough here: there's exactly one thing to wait on
/// (more bytes from the socket), so "process everything already
/// buffered" and "go get more" fold into one match arm rather than
/// needing a separate inner loop to drain pipelined commands before
/// falling through to the next read. That stops being true once there's
/// a second event source to wait on alongside the socket (stage 9's
/// Pub/Sub pushes) — that's a `tokio::select!` between two sources, a
/// genuinely different shape, not just this loop rewritten.
pub fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 4096];
    // Bytes at the front of `buf` already parsed and replied to, but not
    // yet physically removed. `Vec::drain(0..n)` shifts everything after
    // `n` down to fill the gap, so draining after *every* command in a
    // heavily pipelined batch is O(batch size²) overall. Compacting once
    // per socket read instead — draining the whole processed prefix in
    // one go, right before blocking for more bytes — keeps the total
    // work linear in the number of bytes actually processed.
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
                let reply = execute(&command);
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

/// The default cap on concurrent connections `run()` uses — see
/// `run_with_limit`'s doc comment for why one exists at all (same
/// reasoning as stage 2, which this cap is carried forward from).
const MAX_CONNECTIONS: usize = 1024;

/// A counting semaphore built from std primitives only (no external
/// crate — stages 1-6 stay dependency-free by design), identical to
/// stage 2's. See that stage's notes for the full reasoning; kept here
/// verbatim since each stage is an independent crate.
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

/// Accepts connections forever, one thread per connection (same
/// concurrency model as stage 2 — this stage's new ground is the
/// protocol, not the networking). Caps concurrent connections at
/// [`MAX_CONNECTIONS`]; see `run_with_limit`.
pub fn run(listener: TcpListener) -> io::Result<()> {
    run_with_limit(listener, MAX_CONNECTIONS)
}

/// Same as `run`, but with an explicit cap on concurrent connections.
/// Without one, every accepted connection gets its own OS thread with no
/// limit at all: each thread reserves a real stack, and `thread::spawn`
/// doesn't return a `Result` — it panics if the OS refuses to create the
/// thread, on the same thread running this accept loop, taking every
/// already-connected client down with it. Capping concurrency turns
/// that crash into ordinary backpressure instead.
pub fn run_with_limit(listener: TcpListener, max_connections: usize) -> io::Result<()> {
    let semaphore = Arc::new(Semaphore::new(max_connections));
    for stream in listener.incoming() {
        // Scoped the same way a failed connection is, below: log it and
        // keep serving, rather than letting one bad accept take the
        // whole server down via `?`.
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
    use std::time::Duration;

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
