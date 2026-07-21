pub mod command;
pub mod resp;
pub mod store;

use command::{ReadResult, execute, read_command};
use resp::Reply;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How often the background sweep checks for expired keys — unchanged
/// from stage 5/6; `tokio::time::sleep` takes the same `Duration`
/// `std::thread::sleep` did, so this constant didn't need to change.
const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// Same buffer-and-parse loop as every previous stage — the only thing
/// that changed is that `read`/`write_all` are `.await`ed instead of
/// blocking the calling thread. That's the whole rewrite from the
/// connection's point of view: identical protocol handling, a different
/// way of waiting for the socket to be ready.
///
/// `store` is `Arc<Store>` rather than `&Store` because this function's
/// caller hands it to `tokio::spawn`, which requires everything the
/// spawned future captures to be `'static` — a borrowed reference into
/// the caller's stack frame doesn't qualify, an owned `Arc` clone does.
pub async fn handle_connection(mut stream: TcpStream, store: Arc<Store>) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 4096];

    loop {
        loop {
            match read_command(&buf) {
                Ok(ReadResult::Incomplete) => break,
                Ok(ReadResult::Empty { consumed }) => {
                    buf.drain(0..consumed);
                }
                Ok(ReadResult::Command { command, consumed }) => {
                    // `execute` is synchronous and returns before this
                    // `.await` — the store's internal lock is never held
                    // across it. See store.rs for why that's exactly
                    // what keeps `std::sync::Mutex` correct here.
                    let reply = execute(&command, &store);
                    stream.write_all(&reply.encode()).await?;
                    buf.drain(0..consumed);
                }
                Err(e) => {
                    let reply = Reply::Error(format!("ERR Protocol error: {e}"));
                    stream.write_all(&reply.encode()).await?;
                    return Ok(());
                }
            }
        }

        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&read_buf[..n]);
    }
}

/// Accepts connections forever, spawning one **async task** per
/// connection instead of stage 2-6's one **OS thread** per connection —
/// plus a second background task, also spawned rather than a spawned
/// thread, that sweeps expired keys on a timer. Under tokio's default
/// multi-threaded runtime these tasks are still free to run on separate
/// OS threads in parallel (this isn't single-threaded event-loop-only
/// concurrency) — the difference from stage 2-6 is that a task blocked
/// on I/O yields the thread it was running on back to the scheduler
/// instead of parking that thread until the I/O completes, so many more
/// tasks than there are OS threads can be in flight at once.
pub async fn run(listener: TcpListener) -> io::Result<()> {
    let store = Arc::new(Store::new());

    {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(SWEEP_INTERVAL).await;
                store.sweep_expired();
            }
        });
    }

    loop {
        let (stream, _) = listener.accept().await?;
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
