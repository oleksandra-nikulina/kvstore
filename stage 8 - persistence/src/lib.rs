pub mod command;
pub mod persistence;
pub mod resp;
pub mod store;

use command::{ReadResult, execute, read_command};
use persistence::Aof;
use resp::Reply;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// The default cap on concurrent connections `run()` uses — same
/// reasoning as stage 7's `tokio::sync::Semaphore`.
const MAX_CONNECTIONS: usize = 1024;

/// Same buffer-and-parse loop as stage 7, collapsed to one flat loop
/// with cursor-based compaction the same way (see stage 7's notes). The
/// one addition: a command that mutates the store (`command::aof_args`
/// returns `Some`) goes through `Aof::execute_and_log` instead of a
/// bare `execute`, so it's durable-ish (see that method's doc comment
/// on what "durable-ish" means here) before the reply goes back to the
/// client. Read commands skip the AOF's lock entirely — there's nothing
/// for them to log.
pub async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<Store>,
    aof: Arc<Aof>,
) -> io::Result<()> {
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
                let reply = match command::aof_args(&command) {
                    Some(args) => aof.execute_and_log(&command, &args, &store).await,
                    None => execute(&command, &store),
                };
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

/// Unlike stages 1-7, `run` doesn't construct the `Store` itself — by
/// the time this is called, `main.rs` has already replayed the AOF into
/// it, and constructing an empty one here would silently discard that.
/// Caps concurrent connections at [`MAX_CONNECTIONS`]; see stage 2's
/// `run_with_limit` notes for why a cap exists at all.
pub async fn run(listener: TcpListener, store: Arc<Store>, aof: Arc<Aof>) -> io::Result<()> {
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
        let aof = Arc::clone(&aof);
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's whole lifetime
            if let Err(e) = handle_connection(stream, store, aof).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
