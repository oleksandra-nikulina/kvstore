//! Integration test for the thing this stage is actually for: a server
//! that writes some keys, "crashes" (its in-memory `Store` and task set
//! are simply dropped — the same end state a killed process leaves
//! behind, from the AOF's point of view, since nothing more gets
//! flushed to it either way), and comes back up with the same data.
//! There's no literal separate OS process here (that's what `main.rs`
//! is for) — this drives the exact same `replay` + `Aof::open` + `run`
//! sequence `main.rs` does, against the same file path, twice.

use kvstore_stage8::persistence::{Aof, replay};
use kvstore_stage8::run;
use kvstore_stage8::store::Store;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn temp_aof_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("kvstore-stage8-integration-{label}-{nanos}.aof"))
}

/// Starts a server instance against `aof_path`, replaying whatever's
/// already there first — exactly what `main.rs` does on every launch,
/// "first ever start" and "restart after a crash" alike.
async fn start_server(aof_path: &Path) -> SocketAddr {
    let store = Arc::new(Store::new());
    replay(aof_path, &store).await.unwrap();
    let aof = Arc::new(Aof::open(aof_path).await.unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { run(listener, store, aof).await.unwrap() });
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
async fn data_survives_a_simulated_restart() {
    let aof_path = temp_aof_path("restart");

    let addr_a = start_server(&aof_path).await;
    let mut stream_a = connect(addr_a).await;
    send(&mut stream_a, &[b"SET", b"foo", b"bar"]).await;
    assert_eq!(read_reply(&mut stream_a).await, b"+OK\r\n");
    send(&mut stream_a, &[b"RPUSH", b"mylist", b"a", b"b"]).await;
    assert_eq!(read_reply(&mut stream_a).await, b":2\r\n");
    drop(stream_a);

    // "Restart": a brand new Store, replaying from the same AOF path.
    let addr_b = start_server(&aof_path).await;
    let mut stream_b = connect(addr_b).await;

    send(&mut stream_b, &[b"GET", b"foo"]).await;
    assert_eq!(read_reply(&mut stream_b).await, b"$3\r\nbar\r\n");

    send(&mut stream_b, &[b"LRANGE", b"mylist", b"0", b"-1"]).await;
    // read_reply only handles top-level $/simple/error/integer; read the
    // array reply's header + two bulk elements by hand here.
    let header = read_line(&mut stream_b).await;
    assert_eq!(header, b"*2\r\n");
    assert_eq!(read_reply(&mut stream_b).await, b"$1\r\na\r\n");
    assert_eq!(read_reply(&mut stream_b).await, b"$1\r\nb\r\n");

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn writes_after_a_restart_are_appended_not_overwritten() {
    let aof_path = temp_aof_path("append-after-restart");

    let addr_a = start_server(&aof_path).await;
    let mut stream_a = connect(addr_a).await;
    send(&mut stream_a, &[b"SET", b"first", b"1"]).await;
    assert_eq!(read_reply(&mut stream_a).await, b"+OK\r\n");
    drop(stream_a);

    let addr_b = start_server(&aof_path).await;
    let mut stream_b = connect(addr_b).await;
    send(&mut stream_b, &[b"SET", b"second", b"2"]).await;
    assert_eq!(read_reply(&mut stream_b).await, b"+OK\r\n");
    drop(stream_b);

    // A third "restart" should see both keys — proves the second
    // server appended to the existing log instead of truncating it.
    let addr_c = start_server(&aof_path).await;
    let mut stream_c = connect(addr_c).await;
    send(&mut stream_c, &[b"GET", b"first"]).await;
    assert_eq!(read_reply(&mut stream_c).await, b"$1\r\n1\r\n");
    send(&mut stream_c, &[b"GET", b"second"]).await;
    assert_eq!(read_reply(&mut stream_c).await, b"$1\r\n2\r\n");

    let _ = std::fs::remove_file(&aof_path);
}

/// Documents a known, deliberate limitation rather than hiding it: `SET`
/// then `PEXPIRE k 20` only ever gets logged as those two commands, with
/// no record of *when* the expiry was set relative to replay. So after
/// a restart, replaying them rearms a *fresh* 20ms TTL starting from
/// replay time — the key predictably reappears for a moment even though
/// it had already expired (and was lazily deleted) before the restart —
/// and then expires again shortly after. See `command.rs`'s `aof_args`
/// doc comment for why this stage doesn't fix that (real Redis avoids
/// it by logging absolute expiry timestamps instead of relative ones).
#[tokio::test]
async fn a_replayed_relative_ttl_rearms_from_restart_time_not_original_expiry() {
    let aof_path = temp_aof_path("expiry-restart");

    let addr_a = start_server(&aof_path).await;
    let mut stream_a = connect(addr_a).await;
    send(&mut stream_a, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream_a).await, b"+OK\r\n");
    send(&mut stream_a, &[b"PEXPIRE", b"k", b"20"]).await;
    assert_eq!(read_reply(&mut stream_a).await, b":1\r\n");
    tokio::time::sleep(Duration::from_millis(60)).await;
    // Lazily expired here, well before the "restart" — a read-only GET
    // triggers it, but reads are never logged, so the AOF has no record
    // this ever happened.
    send(&mut stream_a, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream_a).await, b"$-1\r\n");
    drop(stream_a);

    let addr_b = start_server(&aof_path).await;
    let mut stream_b = connect(addr_b).await;

    // Immediately after restart: replay just rearmed the TTL, so the
    // key is back.
    send(&mut stream_b, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream_b).await, b"$1\r\nv\r\n");

    // ...and expires again shortly after, same as any other 20ms TTL.
    tokio::time::sleep(Duration::from_millis(60)).await;
    send(&mut stream_b, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream_b).await, b"$-1\r\n");

    let _ = std::fs::remove_file(&aof_path);
}
