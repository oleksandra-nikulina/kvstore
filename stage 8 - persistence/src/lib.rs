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

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// Same buffer-and-parse loop as stage 7. The one addition: a command
/// that mutates the store (`command::aof_args` returns `Some`) goes
/// through `Aof::execute_and_log` instead of a bare `execute`, so it's
/// durable-ish (see that method's doc comment on what "durable-ish"
/// means here) before the reply goes back to the client. Read commands
/// skip the AOF's lock entirely — there's nothing for them to log.
pub async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<Store>,
    aof: Arc<Aof>,
) -> io::Result<()> {
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
                    let reply = match command::aof_args(&command) {
                        Some(args) => aof.execute_and_log(&command, &args, &store).await,
                        None => execute(&command, &store),
                    };
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

/// Unlike stages 1-7, `run` doesn't construct the `Store` itself — by
/// the time this is called, `main.rs` has already replayed the AOF into
/// it, and constructing an empty one here would silently discard that.
pub async fn run(listener: TcpListener, store: Arc<Store>, aof: Arc<Aof>) -> io::Result<()> {
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
        let aof = Arc::clone(&aof);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store, aof).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
