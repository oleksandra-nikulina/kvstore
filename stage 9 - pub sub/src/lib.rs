pub mod command;
pub mod persistence;
pub mod pubsub;
pub mod resp;
pub mod store;

use command::{Command, ReadResult, aof_args, execute, read_command};
use persistence::Aof;
use pubsub::{CHANNEL_CAPACITY, PubSub, message_push, subscribe_ack};
use resp::{Bytes, Reply};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio::task::JoinHandle;

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// The default cap on concurrent connections `run()` uses — same
/// reasoning as stage 7/8's `tokio::sync::Semaphore`.
const MAX_CONNECTIONS: usize = 1024;

/// Caps how many channels a single connection can be subscribed to at
/// once. Without this, a single pipelined `SUBSCRIBE ch1 ch2 ...` could
/// name up to `MAX_MULTIBULK_LEN` (over a million) channels in one
/// command — each one spawning a forwarder task and creating a
/// broadcast group, all under one connection's single permit. Real
/// usage needs nowhere near this many; the cap only ever bites a
/// pathological or adversarial client.
const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 1000;

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
    // Bounded, matching `CHANNEL_CAPACITY`, not `unbounded_channel`: the
    // broadcast channel each forwarder reads from is already bounded
    // specifically so a slow subscriber can't accumulate unbounded
    // memory (see `pubsub.rs`'s doc comment on `CHANNEL_CAPACITY`) — an
    // unbounded channel here would silently erase that bound the moment
    // a message crosses from the broadcast channel into this one. A
    // slow-reading client now applies real backpressure: a full
    // `push_tx` blocks the forwarder's `send`, which stops it from
    // draining its broadcast receiver, which is exactly what lets that
    // receiver's own bound (and `Lagged` reporting) take over instead
    // of memory growing without limit.
    let (push_tx, mut push_rx) = mpsc::channel::<(String, Bytes)>(CHANNEL_CAPACITY);

    loop {
        // Drain and dispatch every complete command already buffered —
        // same buffer-then-parse-repeatedly shape every earlier stage
        // used — before waiting for more input *or* a pushed message.
        // `pos` tracks how much of `buf` is already consumed without
        // physically removing it yet; compacting once per outer
        // iteration (right before the `select!`) rather than once per
        // command keeps this linear in bytes processed, not quadratic
        // in a heavily pipelined batch's size.
        let mut pos = 0;
        loop {
            match read_command(&buf[pos..]) {
                Ok(ReadResult::Incomplete) => break,
                Ok(ReadResult::Empty { consumed }) => {
                    pos += consumed;
                }
                Ok(ReadResult::Command { command, consumed }) => {
                    dispatch(
                        &mut stream,
                        &command,
                        store,
                        aof,
                        pubsub,
                        subscriptions,
                        &push_tx,
                    )
                    .await?;
                    pos += consumed;
                }
                Err(e) => {
                    let reply = Reply::Error(format!("ERR Protocol error: {e}"));
                    stream.write_all(&reply.encode()).await?;
                    return Ok(());
                }
            }
        }
        if pos > 0 {
            buf.drain(0..pos);
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
                    // A message for `channel` can already be sitting in
                    // `push_rx`'s queue at the moment this connection
                    // unsubscribes from it: `handle.abort()` stops the
                    // forwarder from pulling any *more* messages, but
                    // does nothing to reach back into the queue for one
                    // it already handed off before the abort landed.
                    // Re-checking membership here — not just aborting
                    // the forwarder — is what actually prevents a
                    // message arriving after this connection's own
                    // UNSUBSCRIBE ack was already sent for that channel.
                    if subscriptions.contains_key(&channel) {
                        let reply = message_push(&channel, &payload);
                        stream.write_all(&reply.encode()).await?;
                    }
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
    push_tx: &mpsc::Sender<(String, Bytes)>,
) -> io::Result<()> {
    let in_subscribe_mode = !subscriptions.is_empty();
    let allowed_while_subscribed = matches!(
        command,
        Command::Subscribe(_) | Command::Unsubscribe(_) | Command::Ping(_)
    );
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
                    if subscriptions.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
                        let reply = Reply::Error(format!(
                            "ERR too many subscriptions on this connection (max {MAX_SUBSCRIPTIONS_PER_CONNECTION})"
                        ));
                        stream.write_all(&reply.encode()).await?;
                        continue;
                    }
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
            stream
                .write_all(&Reply::Integer(count as i64).encode())
                .await?;
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
    push_tx: mpsc::Sender<(String, Bytes)>,
) -> JoinHandle<()> {
    let mut rx = pubsub.subscribe(channel);
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(message) => {
                    // Awaiting a bounded `send` is the actual
                    // backpressure mechanism: if `serve()`'s consumer
                    // side is slow, this blocks here instead of
                    // draining `rx` further, so the broadcast channel's
                    // own bound (and `Lagged` reporting) absorbs a slow
                    // reader rather than this task buffering unbounded
                    // messages on its behalf.
                    if push_tx.send(message).await.is_err() {
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
/// background expiry sweep from stage 7/8, unchanged. Caps concurrent
/// connections at [`MAX_CONNECTIONS`]; see stage 2's `run_with_limit`
/// notes for why a cap exists at all.
pub async fn run(
    listener: TcpListener,
    store: Arc<Store>,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
) -> io::Result<()> {
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
        let pubsub = Arc::clone(&pubsub);
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's whole lifetime
            if let Err(e) = handle_connection(stream, store, aof, pubsub).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
