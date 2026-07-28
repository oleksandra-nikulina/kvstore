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
use tokio::sync::Semaphore;

/// How often the background sweep checks for expired keys — unchanged
/// from stage 5/6; `tokio::time::sleep` takes the same `Duration`
/// `std::thread::sleep` did, so this constant didn't need to change.
const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// The default cap on concurrent connections `run()` uses — same
/// reasoning as stages 2-6's hand-rolled `Semaphore`, just using
/// `tokio::sync::Semaphore` now that a real async runtime is available:
/// a blocking `std::sync::Condvar`-based wait would park an OS thread
/// instead of yielding the task, defeating the point of being async.
const MAX_CONNECTIONS: usize = 1024;

/// One flat loop, not two nested ones: there's exactly one thing to
/// wait on (more bytes from the socket), so "process everything already
/// buffered" and "go get more" fold into one match arm. `pos` tracks how
/// much of `buf` has already been parsed and replied to without
/// physically removing it yet — compacting once per socket read (right
/// before blocking for more) instead of once per command keeps draining
/// linear in the number of bytes processed, rather than quadratic in a
/// heavily pipelined batch's size.
///
/// `store` is `Arc<Store>` rather than `&Store` because this function's
/// caller hands it to `tokio::spawn`, which requires everything the
/// spawned future captures to be `'static` — a borrowed reference into
/// the caller's stack frame doesn't qualify, an owned `Arc` clone does.
pub async fn handle_connection(mut stream: TcpStream, store: Arc<Store>) -> io::Result<()> {
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
                let n = stream.read(&mut read_buf).await?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&read_buf[..n]);
            }
            Ok(ReadResult::Empty { consumed }) => {
                pos += consumed;
            }
            Ok(ReadResult::Command { command, consumed }) => {
                // `execute` is synchronous and returns before this
                // `.await` — the store's internal lock is never held
                // across it. See store.rs for why that's exactly
                // what keeps `std::sync::RwLock` correct here.
                let reply = execute(&command, &store);
                stream.write_all(&reply.encode()).await?;
                pos += consumed;
            }
            Err(e) => {
                let reply = Reply::Error(format!("ERR Protocol error: {e}"));
                stream.write_all(&reply.encode()).await?;
                return Ok(());
            }
        }
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
/// tasks than there are OS threads can be in flight at once. Caps
/// concurrent connections at [`MAX_CONNECTIONS`] regardless, since
/// "many more" still isn't "unlimited" — see stage 2's `run_with_limit`
/// notes for why a cap exists at all.
pub async fn run(listener: TcpListener) -> io::Result<()> {
    let store = Arc::new(Store::new());
    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

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
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                eprintln!("accept error: {e}");
                // A persistently failing accept() would otherwise
                // busy-spin this loop.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let store = Arc::clone(&store);
        // `acquire_owned` (not `acquire`) so the permit is a value the
        // spawned task can own outright — a permit borrowed from
        // `&semaphore` would tie its lifetime to this loop's stack
        // frame, which doesn't satisfy `tokio::spawn`'s `'static` bound.
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's whole lifetime
            if let Err(e) = handle_connection(stream, store).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
