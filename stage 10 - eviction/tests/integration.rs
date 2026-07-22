use kvstore_stage10::eviction::Policy;
use kvstore_stage10::persistence::Aof;
use kvstore_stage10::pubsub::PubSub;
use kvstore_stage10::run;
use kvstore_stage10::store::Store;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn temp_aof_path(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("kvstore-stage10-test-{label}-{nanos}.aof"))
}

async fn spawn_server(store: Store) -> SocketAddr {
    let path = temp_aof_path("server");
    let aof = Arc::new(Aof::open(&path).await.unwrap());
    let pubsub = Arc::new(PubSub::new());
    let store = Arc::new(store);

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

async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
    let line = read_line(stream).await;
    match line[0] {
        b'$' => {
            let len: i64 = std::str::from_utf8(&line[1..line.len() - 2])
                .unwrap()
                .parse()
                .unwrap();
            let mut out = line;
            if len >= 0 {
                let mut payload = vec![0u8; len as usize + 2];
                stream.read_exact(&mut payload).await.unwrap();
                out.extend(payload);
            }
            out
        }
        _ => line,
    }
}

#[tokio::test]
async fn a_store_with_no_configured_limit_never_evicts_over_the_wire() {
    let addr = spawn_server(Store::new()).await;
    let mut stream = connect(addr).await;
    for i in 0..200 {
        let key = format!("k{i}");
        send(&mut stream, &[b"SET", key.as_bytes(), &[0u8; 50]]).await;
        assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    }
    // Spot-check the very first key written is still there.
    send(&mut stream, &[b"GET", b"k0"]).await;
    let reply = read_reply(&mut stream).await;
    assert!(reply.starts_with(b"$50\r\n"));
}

#[tokio::test]
async fn lru_evicts_the_oldest_untouched_key_once_over_the_cap() {
    // Each "kN" key: 2-byte key + 20-byte value = 22 bytes. Cap of 70
    // fits 3 comfortably (66 bytes); a 4th forces exactly one eviction.
    let addr = spawn_server(Store::with_eviction(70, Policy::Lru)).await;
    let mut stream = connect(addr).await;

    for i in 0..3 {
        let key = format!("k{i}");
        send(&mut stream, &[b"SET", key.as_bytes(), &[0u8; 20]]).await;
        assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    }
    send(&mut stream, &[b"SET", b"k3", &[0u8; 20]]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");

    send(&mut stream, &[b"GET", b"k0"]).await;
    assert_eq!(
        read_reply(&mut stream).await,
        b"$-1\r\n",
        "k0 (least recently touched) should have been evicted"
    );
    send(&mut stream, &[b"GET", b"k3"]).await;
    assert!(read_reply(&mut stream).await.starts_with(b"$20\r\n"));
}

/// The stage's core demonstration, driven over real sockets: the same
/// access pattern leaves LRU and LFU evicting different keys.
#[tokio::test]
async fn lru_and_lfu_disagree_over_the_wire_after_a_scan() {
    for (policy, hot_survives) in [(Policy::Lru, false), (Policy::Lfu, true)] {
        // "hot"(3)+10=13; "scan-N"(6)+10=16 each; 4 keys = 61 bytes.
        // Cap of 70 fits all four without evicting during setup.
        let addr = spawn_server(Store::with_eviction(70, policy)).await;
        let mut stream = connect(addr).await;

        send(&mut stream, &[b"SET", b"hot", &[0u8; 10]]).await;
        read_reply(&mut stream).await;
        for i in 0..3 {
            let key = format!("scan-{i}");
            send(&mut stream, &[b"SET", key.as_bytes(), &[0u8; 10]]).await;
            read_reply(&mut stream).await;
        }
        for _ in 0..20 {
            send(&mut stream, &[b"GET", b"hot"]).await;
            read_reply(&mut stream).await;
        }
        for i in 0..3 {
            let key = format!("scan-{i}");
            send(&mut stream, &[b"GET", key.as_bytes()]).await;
            read_reply(&mut stream).await;
        }

        // "new-key"(7)+10=17; 61+17=78, over the 70 cap - one eviction
        // (any 16-byte scan key) is enough to land back under it.
        send(&mut stream, &[b"SET", b"new-key", &[0u8; 10]]).await;
        assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");

        send(&mut stream, &[b"GET", b"hot"]).await;
        let reply = read_reply(&mut stream).await;
        if hot_survives {
            assert!(
                reply.starts_with(b"$10\r\n"),
                "{policy:?}: expected 'hot' to survive, got {reply:?}"
            );
        } else {
            assert_eq!(reply, b"$-1\r\n", "{policy:?}: expected 'hot' to be evicted");
        }
    }
}

#[tokio::test]
async fn writing_a_value_that_alone_exceeds_maxmemory_still_succeeds() {
    let addr = spawn_server(Store::with_eviction(5, Policy::Lru)).await;
    let mut stream = connect(addr).await;
    send(&mut stream, &[b"SET", b"k", &[0u8; 100]]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"GET", b"k"]).await;
    assert!(read_reply(&mut stream).await.starts_with(b"$100\r\n"));
}
