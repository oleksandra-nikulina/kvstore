pub mod command;
pub mod eviction;
pub mod persistence;
pub mod pubsub;
pub mod resp;
pub mod store;

use command::{Command, ReadResult, aof_args, execute, read_command};
use persistence::Aof;
use pubsub::{PubSub, message_push, subscribe_ack};
use resp::{Bytes, Reply};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// Every command this connection is currently subscribed to, and the
/// forwarder task relaying that channel's broadcast messages into this
/// connection's `push_tx`. Non-empty ⇒ this connection is "in
/// subscribe mode" and most other commands are rejected, same as real
/// Redis's RESP2 pub/sub restriction.
type Subscriptions = HashMap<String, JoinHandle<()>>;

pub async fn handle_connection(
    stream: TcpStream,
    store: Arc<Store>,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
) -> io::Result<()> {
    let mut subscriptions: Subscriptions = HashMap::new();
    let result = serve(stream, &store, &aof, &pubsub, &mut subscriptions).await;

    // Whatever ended the connection — clean disconnect, protocol error,
    // I/O error — every forwarder task this connection ever started has
    // to be stopped and its channel entry cleaned up, or a client that
    // subscribed and vanished would leak a task and a broadcast group
    // forever. Running this once, after `serve` returns by any path
    // (instead of duplicating cleanup at every early return inside it),
    // is the whole reason `serve` is a separate function.
    //
    // `abort()` only *requests* cancellation — the task (and the
    // `broadcast::Receiver` it owns) isn't actually torn down until the
    // runtime next polls it, so `receiver_count()` can still read >0
    // for an instant afterward. `cleanup_if_unused` would then wrongly
    // see the channel as still in use and leave its (now genuinely
    // dead) entry in the map forever. Awaiting the handle blocks until
    // the task has actually finished unwinding — its `Receiver` is
    // dropped as part of that — so `cleanup_if_unused` sees an accurate
    // count. (Found by code review: `receiver_count()` read 1, not 0,
    // immediately after a bare `abort()`.)
    for (channel, handle) in subscriptions.drain() {
        handle.abort();
        let _ = handle.await;
        pubsub.cleanup_if_unused(&channel);
    }

    result
}

async fn serve(
    mut stream: TcpStream,
    store: &Arc<Store>,
    aof: &Arc<Aof>,
    pubsub: &Arc<PubSub>,
    subscriptions: &mut Subscriptions,
) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 4096];
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<(String, Bytes)>();

    loop {
        // Drain and dispatch every complete command already buffered —
        // same buffer-then-parse-repeatedly shape every earlier stage
        // used — before waiting for more input *or* a pushed message.
        loop {
            match read_command(&buf) {
                Ok(ReadResult::Incomplete) => break,
                Ok(ReadResult::Empty { consumed }) => {
                    buf.drain(0..consumed);
                }
                Ok(ReadResult::Command { command, consumed }) => {
                    dispatch(&mut stream, &command, store, aof, pubsub, subscriptions, &push_tx)
                        .await?;
                    buf.drain(0..consumed);
                }
                Err(e) => {
                    let reply = Reply::Error(format!("ERR Protocol error: {e}"));
                    stream.write_all(&reply.encode()).await?;
                    return Ok(());
                }
            }
        }

        // The one new idea this stage adds to every earlier
        // connection's read loop: wait on *either* more bytes from the
        // client *or* a message pushed from a `PUBLISH` this connection
        // is subscribed to — whichever happens first. A connection with
        // no subscriptions never receives anything on `push_rx`, so
        // this reduces to exactly the stage 8 loop for those.
        tokio::select! {
            maybe_push = push_rx.recv() => {
                if let Some((channel, payload)) = maybe_push {
                    let reply = message_push(&channel, &payload);
                    stream.write_all(&reply.encode()).await?;
                }
            }
            read_result = stream.read(&mut read_buf) => {
                let n = read_result?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&read_buf[..n]);
            }
        }
    }
}

/// Executes one already-parsed command and writes its reply (or, for
/// `SUBSCRIBE`/`UNSUBSCRIBE`, replies — one per channel) to `stream`.
///
/// `SUBSCRIBE`/`UNSUBSCRIBE`/`PUBLISH` are handled directly here rather
/// than through `execute()`: they need the connection's local
/// `subscriptions` map and the shared `pubsub` registry, neither of
/// which fits `execute`'s `(command, store) -> Reply` shape — every
/// other command is a pure `Store` operation, these three aren't.
async fn dispatch(
    stream: &mut TcpStream,
    command: &Command,
    store: &Store,
    aof: &Aof,
    pubsub: &PubSub,
    subscriptions: &mut Subscriptions,
    push_tx: &mpsc::UnboundedSender<(String, Bytes)>,
) -> io::Result<()> {
    let in_subscribe_mode = !subscriptions.is_empty();
    let allowed_while_subscribed =
        matches!(command, Command::Subscribe(_) | Command::Unsubscribe(_) | Command::Ping(_));
    if in_subscribe_mode && !allowed_while_subscribed {
        let reply = Reply::Error(
            "ERR only SUBSCRIBE / UNSUBSCRIBE / PING allowed while in pub/sub mode".to_string(),
        );
        return stream.write_all(&reply.encode()).await;
    }

    match command {
        Command::Subscribe(channels) => {
            for channel in channels {
                if !subscriptions.contains_key(channel) {
                    let handle = spawn_forwarder(pubsub, channel, push_tx.clone());
                    subscriptions.insert(channel.clone(), handle);
                }
                let reply = subscribe_ack("subscribe", Some(channel), subscriptions.len());
                stream.write_all(&reply.encode()).await?;
            }
        }
        Command::Unsubscribe(channels) => {
            let targets: Vec<String> = if channels.is_empty() {
                subscriptions.keys().cloned().collect()
            } else {
                channels.clone()
            };
            if targets.is_empty() {
                // Nothing was subscribed to begin with — real Redis
                // still sends exactly one ack for this, with a null
                // channel, rather than silently doing nothing.
                let reply = subscribe_ack("unsubscribe", None, 0);
                stream.write_all(&reply.encode()).await?;
            } else {
                for channel in targets {
                    if let Some(handle) = subscriptions.remove(&channel) {
                        // See the matching comment in `handle_connection`
                        // for why `cleanup_if_unused` has to wait for the
                        // aborted task to actually finish first.
                        handle.abort();
                        let _ = handle.await;
                        pubsub.cleanup_if_unused(&channel);
                    }
                    let reply = subscribe_ack("unsubscribe", Some(&channel), subscriptions.len());
                    stream.write_all(&reply.encode()).await?;
                }
            }
        }
        Command::Publish(channel, message) => {
            let count = pubsub.publish(channel, message.clone());
            stream.write_all(&Reply::Integer(count as i64).encode()).await?;
        }
        _ => {
            let reply = match aof_args(command) {
                Some(args) => aof.execute_and_log(command, &args, store).await,
                None => execute(command, store),
            };
            stream.write_all(&reply.encode()).await?;
        }
    }
    Ok(())
}

/// One task per subscribed channel, doing nothing but relay that
/// channel's broadcast messages into this connection's single
/// `push_tx`. This is what lets `serve`'s `select!` stay a fixed
/// two-way choice (new bytes vs. a pushed message) no matter how many
/// channels the client subscribes to — `select!` needs its branches
/// fixed at compile time, so fanning in an arbitrary, changing number
/// of `broadcast::Receiver`s has to happen through something else
/// first; a small forwarder task per subscription plus one shared
/// `mpsc` channel is that something else.
fn spawn_forwarder(
    pubsub: &PubSub,
    channel: &str,
    push_tx: mpsc::UnboundedSender<(String, Bytes)>,
) -> JoinHandle<()> {
    let mut rx = pubsub.subscribe(channel);
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(message) => {
                    if push_tx.send(message).is_err() {
                        // The connection's serve() loop is gone.
                        break;
                    }
                }
                // This subscriber fell more than CHANNEL_CAPACITY
                // messages behind the publisher(s) and missed some —
                // not fatal, just keep going from where the broadcast
                // buffer now allows.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                // The channel's last Sender is gone — nothing left to
                // ever receive here.
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

/// Accepts connections forever, one `tokio` task per connection, all
/// sharing one `Store`, one `Aof`, and one `PubSub` registry — plus the
/// background expiry sweep from stage 7/8, unchanged.
pub async fn run(
    listener: TcpListener,
    store: Arc<Store>,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
) -> io::Result<()> {
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
        let pubsub = Arc::clone(&pubsub);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store, aof, pubsub).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
