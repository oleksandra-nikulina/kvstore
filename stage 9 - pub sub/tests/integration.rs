use kvstore_stage9::persistence::Aof;
use kvstore_stage9::pubsub::PubSub;
use kvstore_stage9::run;
use kvstore_stage9::store::Store;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn temp_aof_path(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("kvstore-stage9-test-{label}-{nanos}.aof"))
}

async fn spawn_server() -> SocketAddr {
    let path = temp_aof_path("server");
    let store = Arc::new(Store::new());
    let aof = Arc::new(Aof::open(&path).await.unwrap());
    let pubsub = Arc::new(PubSub::new());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { run(listener, store, aof, pubsub).await.unwrap() });
    addr
}

async fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend(format!("${}\r\n", part.len()).into_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn send(stream: &mut TcpStream, parts: &[&[u8]]) {
    stream.write_all(&encode_command(parts)).await.unwrap();
}

async fn read_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).await.unwrap();
        out.push(byte[0]);
        if out.ends_with(b"\r\n") {
            break;
        }
    }
    out
}

async fn read_bulk_body(stream: &mut TcpStream, header: Vec<u8>) -> Vec<u8> {
    let len: i64 = std::str::from_utf8(&header[1..header.len() - 2])
        .unwrap()
        .parse()
        .unwrap();
    let mut out = header;
    if len >= 0 {
        let mut payload = vec![0u8; len as usize + 2];
        stream.read_exact(&mut payload).await.unwrap();
        out.extend(payload);
    }
    out
}

/// Reads one full RESP reply. Every array this project produces
/// (`LRANGE`/`HGETALL`/`SMEMBERS`, and now the pub/sub acks and message
/// pushes) is one level deep and contains only bulk strings or
/// integers, so — same reasoning as stage 7/8's helper — this is
/// written iteratively rather than with real recursion.
async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
    let line = read_line(stream).await;
    match line[0] {
        b'$' => read_bulk_body(stream, line).await,
        b'*' => {
            let count: i64 = std::str::from_utf8(&line[1..line.len() - 2])
                .unwrap()
                .parse()
                .unwrap();
            let mut out = line;
            for _ in 0..count.max(0) {
                let item_line = read_line(stream).await;
                match item_line[0] {
                    b'$' => out.extend(read_bulk_body(stream, item_line).await),
                    _ => out.extend(item_line), // e.g. the `:count` in a subscribe ack
                }
            }
            out
        }
        _ => line,
    }
}

/// Gives the server's background subscriber-forwarder tasks a moment to
/// actually register before a `PUBLISH` is sent — `SUBSCRIBE`'s ack
/// having been received by the test is already a stronger guarantee
/// than this in practice, but a couple of these tests publish from a
/// second connection immediately after, so a tiny grace period avoids
/// any theoretical scheduling race between "ack sent" and "receiver
/// fully registered in the broadcast group."
async fn settle() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[tokio::test]
async fn a_single_subscriber_receives_a_published_message() {
    let addr = spawn_server().await;

    let mut subscriber = connect(addr).await;
    send(&mut subscriber, &[b"SUBSCRIBE", b"news"]).await;
    let ack = read_reply(&mut subscriber).await;
    assert_eq!(
        ack,
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
    );

    settle().await;

    let mut publisher = connect(addr).await;
    send(&mut publisher, &[b"PUBLISH", b"news", b"hello"]).await;
    assert_eq!(read_reply(&mut publisher).await, b":1\r\n");

    let pushed = read_reply(&mut subscriber).await;
    assert_eq!(
        pushed,
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
    );
}

#[tokio::test]
async fn publish_to_a_channel_with_no_subscribers_replies_zero() {
    let addr = spawn_server().await;
    let mut stream = connect(addr).await;
    send(&mut stream, &[b"PUBLISH", b"nobody-listening", b"hi"]).await;
    assert_eq!(read_reply(&mut stream).await, b":0\r\n");
}

/// The stage README's stated test: multiple concurrent subscriber
/// connections to the same channel must *all* receive a published
/// message, not just one of them (this isn't a work queue — every
/// subscriber gets every message).
#[tokio::test(flavor = "multi_thread")]
async fn many_concurrent_subscribers_all_receive_the_same_published_message() {
    let addr = spawn_server().await;
    let subscriber_count = 20;

    let mut subscribers = Vec::new();
    for _ in 0..subscriber_count {
        let mut stream = connect(addr).await;
        send(&mut stream, &[b"SUBSCRIBE", b"broadcast"]).await;
        read_reply(&mut stream).await; // ack
        subscribers.push(stream);
    }

    settle().await;

    let mut publisher = connect(addr).await;
    send(&mut publisher, &[b"PUBLISH", b"broadcast", b"to everyone"]).await;
    let receiver_count = read_reply(&mut publisher).await;
    assert_eq!(receiver_count, format!(":{subscriber_count}\r\n").into_bytes());

    for mut stream in subscribers {
        let pushed = read_reply(&mut stream).await;
        assert_eq!(
            pushed,
            b"*3\r\n$7\r\nmessage\r\n$9\r\nbroadcast\r\n$11\r\nto everyone\r\n"
        );
    }
}

#[tokio::test]
async fn a_connection_can_subscribe_to_multiple_channels_and_gets_both() {
    let addr = spawn_server().await;

    let mut subscriber = connect(addr).await;
    send(&mut subscriber, &[b"SUBSCRIBE", b"news", b"sports"]).await;
    assert_eq!(
        read_reply(&mut subscriber).await,
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
    );
    assert_eq!(
        read_reply(&mut subscriber).await,
        b"*3\r\n$9\r\nsubscribe\r\n$6\r\nsports\r\n:2\r\n"
    );

    settle().await;

    let mut publisher = connect(addr).await;
    send(&mut publisher, &[b"PUBLISH", b"sports", b"score"]).await;
    assert_eq!(read_reply(&mut publisher).await, b":1\r\n");
    send(&mut publisher, &[b"PUBLISH", b"news", b"headline"]).await;
    assert_eq!(read_reply(&mut publisher).await, b":1\r\n");

    // Messages can arrive in either order relative to each other, but
    // each one must be a complete, correctly-framed push.
    let mut received = std::collections::HashSet::new();
    for _ in 0..2 {
        received.insert(read_reply(&mut subscriber).await);
    }
    assert!(received.contains(b"*3\r\n$7\r\nmessage\r\n$6\r\nsports\r\n$5\r\nscore\r\n".as_slice()));
    assert!(received.contains(b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$8\r\nheadline\r\n".as_slice()));
}

#[tokio::test]
async fn unsubscribe_stops_further_delivery() {
    let addr = spawn_server().await;

    let mut subscriber = connect(addr).await;
    send(&mut subscriber, &[b"SUBSCRIBE", b"news"]).await;
    read_reply(&mut subscriber).await; // ack

    send(&mut subscriber, &[b"UNSUBSCRIBE", b"news"]).await;
    assert_eq!(
        read_reply(&mut subscriber).await,
        b"*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n"
    );

    settle().await;

    let mut publisher = connect(addr).await;
    send(&mut publisher, &[b"PUBLISH", b"news", b"too late"]).await;
    assert_eq!(
        read_reply(&mut publisher).await,
        b":0\r\n",
        "no subscribers should remain after UNSUBSCRIBE"
    );

    // The (former) subscriber connection is back to normal command mode.
    send(&mut subscriber, &[b"PING"]).await;
    assert_eq!(read_reply(&mut subscriber).await, b"+PONG\r\n");
}

#[tokio::test]
async fn unsubscribe_with_no_arguments_leaves_all_channels() {
    let addr = spawn_server().await;

    let mut subscriber = connect(addr).await;
    send(&mut subscriber, &[b"SUBSCRIBE", b"a", b"b", b"c"]).await;
    for _ in 0..3 {
        read_reply(&mut subscriber).await;
    }

    send(&mut subscriber, &[b"UNSUBSCRIBE"]).await;
    for _ in 0..3 {
        let ack = read_reply(&mut subscriber).await;
        assert!(ack.starts_with(b"*3\r\n$11\r\nunsubscribe\r\n"));
    }

    // Back to normal mode: an ordinary command works again.
    send(&mut subscriber, &[b"PING"]).await;
    assert_eq!(read_reply(&mut subscriber).await, b"+PONG\r\n");
}

#[tokio::test]
async fn unsubscribe_with_nothing_subscribed_still_sends_one_null_ack() {
    let addr = spawn_server().await;
    let mut stream = connect(addr).await;
    send(&mut stream, &[b"UNSUBSCRIBE"]).await;
    assert_eq!(
        read_reply(&mut stream).await,
        b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
    );
}

#[tokio::test]
async fn most_commands_are_rejected_while_in_subscribe_mode() {
    let addr = spawn_server().await;
    let mut stream = connect(addr).await;
    send(&mut stream, &[b"SUBSCRIBE", b"news"]).await;
    read_reply(&mut stream).await; // ack

    send(&mut stream, &[b"GET", b"anything"]).await;
    let reply = read_reply(&mut stream).await;
    assert!(reply.starts_with(b"-ERR"));
    assert!(String::from_utf8_lossy(&reply).contains("pub/sub mode"));

    // PING still works while subscribed.
    send(&mut stream, &[b"PING"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+PONG\r\n");

    // SUBSCRIBE/UNSUBSCRIBE still work too.
    send(&mut stream, &[b"SUBSCRIBE", b"sports"]).await;
    assert_eq!(
        read_reply(&mut stream).await,
        b"*3\r\n$9\r\nsubscribe\r\n$6\r\nsports\r\n:2\r\n"
    );
}

#[tokio::test]
async fn disconnecting_while_subscribed_cleans_up_so_publish_sees_zero_receivers() {
    let addr = spawn_server().await;

    let mut subscriber = connect(addr).await;
    send(&mut subscriber, &[b"SUBSCRIBE", b"news"]).await;
    read_reply(&mut subscriber).await; // ack
    settle().await;
    drop(subscriber); // simulate a client disconnecting without UNSUBSCRIBE

    // Give the server's cleanup (which runs after handle_connection's
    // serve() loop observes the closed socket) a moment to run.
    settle().await;

    let mut publisher = connect(addr).await;
    send(&mut publisher, &[b"PUBLISH", b"news", b"anybody?"]).await;
    assert_eq!(
        read_reply(&mut publisher).await,
        b":0\r\n",
        "expected the disconnected subscriber to have been cleaned up"
    );
}
