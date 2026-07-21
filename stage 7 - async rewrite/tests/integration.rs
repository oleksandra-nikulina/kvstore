//! The stage 3-6 integration suites, ported to run against this stage's
//! async server: same assertions, same wire behavior — the only real
//! change anywhere in this file is that socket I/O and concurrent
//! clients are driven with `tokio` instead of blocking `std::net` calls
//! and OS threads. That the exact same protocol-level test suite passes
//! unchanged in shape is the point: stage 7 is a rewrite of *how*
//! connections are served, not *what* they're served.

use kvstore_stage7::run;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { run(listener).await.unwrap() });
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

/// Reads exactly one RESP reply. Every `Reply::Array` this project ever
/// produces (`LRANGE`, `HGETALL`, `SMEMBERS`) is one level deep and
/// contains only bulk strings, so this doesn't need general recursion —
/// unlike the sync-server integration tests, which had no reason to
/// avoid recursion, this is written iteratively on purpose since a
/// recursive `async fn` needs boxing (`Pin<Box<dyn Future>>`) to compile
/// at all, and that machinery isn't worth it for a test helper.
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
                out.extend(read_bulk_body(stream, item_line).await);
            }
            out
        }
        _ => line,
    }
}

// ==== ported from stage 3: PING / ECHO / protocol framing ==============

#[tokio::test]
async fn ping_with_no_argument_replies_pong() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"PING"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+PONG\r\n");
}

#[tokio::test]
async fn ping_with_a_message_echoes_it_as_a_bulk_string() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"PING", b"hello"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$5\r\nhello\r\n");
}

#[tokio::test]
async fn echo_replies_with_the_argument_as_a_bulk_string() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"ECHO", b"world"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$5\r\nworld\r\n");
}

#[tokio::test]
async fn unknown_command_gets_an_error_reply_but_the_connection_stays_open() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"FOO"]).await;
    let reply = read_reply(&mut stream).await;
    assert!(reply.starts_with(b"-"));
    assert!(String::from_utf8_lossy(&reply).contains("unknown command"));

    send(&mut stream, &[b"PING"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+PONG\r\n");
}

#[tokio::test]
async fn pipelined_commands_sent_in_one_write_each_get_a_reply() {
    let mut stream = connect(spawn_server().await).await;
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n")
        .await
        .unwrap();
    assert_eq!(read_reply(&mut stream).await, b"+PONG\r\n");
    assert_eq!(read_reply(&mut stream).await, b"$2\r\nhi\r\n");
}

#[tokio::test]
async fn a_command_split_across_multiple_writes_is_still_parsed_correctly() {
    let mut stream = connect(spawn_server().await).await;
    let full = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    for chunk in full.chunks(3) {
        stream.write_all(chunk).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(read_reply(&mut stream).await, b"$5\r\nhello\r\n");
}

#[tokio::test]
async fn a_bulk_string_payload_containing_crlf_round_trips_intact() {
    let mut stream = connect(spawn_server().await).await;
    stream
        .write_all(b"*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n")
        .await
        .unwrap();
    assert_eq!(read_reply(&mut stream).await, b"$4\r\na\r\nb\r\n");
}

#[tokio::test]
async fn malformed_input_gets_an_error_reply_and_the_connection_is_closed() {
    let mut stream = connect(spawn_server().await).await;
    stream.write_all(b"not resp at all\r\n").await.unwrap();

    let reply = read_reply(&mut stream).await;
    assert!(reply.starts_with(b"-"));

    let mut buf = [0u8; 16];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "expected the connection to be closed");
}

// ==== ported from stage 4: GET / SET / DEL ==============================

#[tokio::test]
async fn get_on_a_missing_key_replies_a_null_bulk_string() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"GET", b"nope"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$-1\r\n");
}

#[tokio::test]
async fn set_then_get_round_trips_over_the_wire() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"foo", b"bar"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"GET", b"foo"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$3\r\nbar\r\n");
}

#[tokio::test]
async fn del_replies_the_count_of_keys_that_actually_existed() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"a", b"1"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"DEL", b"a", b"missing"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"GET", b"a"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$-1\r\n");
}

#[tokio::test]
async fn a_value_persists_across_separate_connections() {
    let addr = spawn_server().await;

    let mut writer = connect(addr).await;
    send(&mut writer, &[b"SET", b"shared-key", b"visible-everywhere"]).await;
    assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

    let mut reader = connect(addr).await;
    send(&mut reader, &[b"GET", b"shared-key"]).await;
    assert_eq!(
        read_reply(&mut reader).await,
        b"$18\r\nvisible-everywhere\r\n"
    );
}

/// Same test as stage 4's, but every "client" is a spawned `tokio` task
/// instead of a spawned `std::thread` — 50 of these would already be a
/// noticeable amount of OS thread/stack overhead; as tasks, it's
/// unremarkable.
#[tokio::test(flavor = "multi_thread")]
async fn many_concurrent_clients_set_and_get_their_own_key_without_interference() {
    let addr = spawn_server().await;
    let client_count = 50;

    let handles: Vec<_> = (0..client_count)
        .map(|i| {
            tokio::spawn(async move {
                let mut stream = connect(addr).await;
                let key = format!("client-{i:03}");
                let value = format!("value-{i:03}");

                send(&mut stream, &[b"SET", key.as_bytes(), value.as_bytes()]).await;
                assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");

                send(&mut stream, &[b"GET", key.as_bytes()]).await;
                let expected = format!("${}\r\n{value}\r\n", value.len());
                assert_eq!(
                    read_reply(&mut stream).await,
                    expected.as_bytes(),
                    "client {i} read back the wrong value"
                );
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Same race-proof as stage 4's, ported to tasks: many separate client
/// connections racing to `SET` the same key must still leave exactly one
/// writer's full value behind, never a torn mix.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_clients_racing_on_a_shared_key_never_produce_a_torn_value() {
    let addr = spawn_server().await;
    let writer_count = 32;

    let handles: Vec<_> = (0..writer_count)
        .map(|i| {
            tokio::spawn(async move {
                let mut stream = connect(addr).await;
                let value = vec![i as u8; 64];
                send(&mut stream, &[b"SET", b"shared", &value]).await;
                assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
            })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }

    let mut reader = connect(addr).await;
    send(&mut reader, &[b"GET", b"shared"]).await;
    let reply = read_reply(&mut reader).await;
    let value = &reply[5..reply.len() - 2]; // strip "$64\r\n" .. "\r\n"
    assert!(
        value.iter().all(|&b| b == value[0]),
        "value contains a mix of bytes from different writers: {value:?}"
    );
    assert!((0..writer_count as u8).contains(&value[0]));
}

// ==== ported from stage 5: expiration ===================================

#[tokio::test]
async fn ttl_is_minus_one_for_a_key_with_no_expiry_and_minus_two_when_missing() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"TTL", b"nope"]).await;
    assert_eq!(read_reply(&mut stream).await, b":-2\r\n");
    send(&mut stream, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"TTL", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b":-1\r\n");
}

#[tokio::test]
async fn expire_sets_a_ttl_that_ttl_then_reports() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"EXPIRE", b"k", b"60"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"TTL", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b":60\r\n");
}

#[tokio::test]
async fn persist_removes_a_ttl_so_the_key_survives_past_it() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"PEXPIRE", b"k", b"50"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"PERSIST", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");

    tokio::time::sleep(Duration::from_millis(200)).await;
    send(&mut stream, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$1\r\nv\r\n");
}

#[tokio::test]
async fn a_key_disappears_on_lazy_read_after_its_ttl_elapses() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"PEXPIRE", b"k", b"20"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");

    tokio::time::sleep(Duration::from_millis(60)).await;

    send(&mut stream, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$-1\r\n");
}

#[tokio::test]
async fn an_unread_expired_key_is_removed_by_the_background_sweep() {
    let addr = spawn_server().await;

    let mut writer = connect(addr).await;
    send(&mut writer, &[b"SET", b"ghost", b"boo"]).await;
    assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");
    send(&mut writer, &[b"PEXPIRE", b"ghost", b"10"]).await;
    assert_eq!(read_reply(&mut writer).await, b":1\r\n");
    drop(writer);

    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut checker = connect(addr).await;
    send(&mut checker, &[b"DEL", b"ghost"]).await;
    assert_eq!(
        read_reply(&mut checker).await,
        b":0\r\n",
        "expected the background sweep task to have already removed the key"
    );
}

#[tokio::test]
async fn setting_a_key_again_clears_its_previous_ttl_over_the_wire() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"v1"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"EXPIRE", b"k", b"60"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"SET", b"k", b"v2"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
    send(&mut stream, &[b"TTL", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b":-1\r\n");
}

#[tokio::test]
async fn a_non_integer_ttl_argument_is_a_clean_error_not_a_crash() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");

    send(&mut stream, &[b"EXPIRE", b"k", b"soon"]).await;
    let reply = read_reply(&mut stream).await;
    assert_eq!(reply[0], b'-');
    assert!(String::from_utf8_lossy(&reply).contains("not an integer"));

    send(&mut stream, &[b"PING"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+PONG\r\n");
}

/// Regression test for the overflow-panic bug found in review before
/// this stage was built (see the project's `REVIEW_NOTES.md`): a huge
/// `EXPIRE` TTL must be a clean error, not a crashed connection, on the
/// async server too.
#[tokio::test]
async fn an_absurdly_large_expire_ttl_is_a_clean_error_not_a_crashed_connection() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"v"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");

    send(&mut stream, &[b"EXPIRE", b"k", b"9223372036854775807"]).await;
    let reply = read_reply(&mut stream).await;
    assert_eq!(reply[0], b'-');
    assert!(String::from_utf8_lossy(&reply).contains("invalid expire time"));

    send(&mut stream, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$1\r\nv\r\n");
}

// ==== ported from stage 6: data types ===================================

#[tokio::test]
async fn lpush_rpush_lrange_lpop_over_the_wire() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"RPUSH", b"l", b"a", b"b"]).await;
    assert_eq!(read_reply(&mut stream).await, b":2\r\n");
    send(&mut stream, &[b"LPUSH", b"l", b"z"]).await;
    assert_eq!(read_reply(&mut stream).await, b":3\r\n");
    send(&mut stream, &[b"LRANGE", b"l", b"0", b"-1"]).await;
    assert_eq!(
        read_reply(&mut stream).await,
        b"*3\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    send(&mut stream, &[b"LPOP", b"l"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$1\r\nz\r\n");
}

#[tokio::test]
async fn hset_hget_hdel_over_the_wire() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"HSET", b"h", b"f1", b"v1"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"HSET", b"h", b"f1", b"v2"]).await;
    assert_eq!(read_reply(&mut stream).await, b":0\r\n");
    send(&mut stream, &[b"HGET", b"h", b"f1"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$2\r\nv2\r\n");
    send(&mut stream, &[b"HDEL", b"h", b"f1"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"HGET", b"h", b"f1"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$-1\r\n");
}

#[tokio::test]
async fn sadd_sismember_srem_over_the_wire() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SADD", b"s", b"a", b"b", b"a"]).await;
    assert_eq!(read_reply(&mut stream).await, b":2\r\n");
    send(&mut stream, &[b"SISMEMBER", b"s", b"a"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"SREM", b"s", b"a"]).await;
    assert_eq!(read_reply(&mut stream).await, b":1\r\n");
    send(&mut stream, &[b"SISMEMBER", b"s", b"a"]).await;
    assert_eq!(read_reply(&mut stream).await, b":0\r\n");
}

#[tokio::test]
async fn wrongtype_error_over_the_wire_and_the_connection_stays_usable() {
    let mut stream = connect(spawn_server().await).await;
    send(&mut stream, &[b"SET", b"k", b"a string"]).await;
    assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");

    send(&mut stream, &[b"LPUSH", b"k", b"x"]).await;
    assert!(read_reply(&mut stream).await.starts_with(b"-WRONGTYPE"));

    send(&mut stream, &[b"SADD", b"k", b"x"]).await;
    assert!(read_reply(&mut stream).await.starts_with(b"-WRONGTYPE"));

    send(&mut stream, &[b"GET", b"k"]).await;
    assert_eq!(read_reply(&mut stream).await, b"$8\r\na string\r\n");
}

/// Same push-loss check as stage 6's, ported to tasks.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_clients_pushing_to_the_same_list_lose_no_pushes() {
    let addr = spawn_server().await;
    let pusher_count = 40;

    let handles: Vec<_> = (0..pusher_count)
        .map(|i| {
            tokio::spawn(async move {
                let mut stream = connect(addr).await;
                let value = format!("item-{i:03}");
                send(&mut stream, &[b"RPUSH", b"shared-list", value.as_bytes()]).await;
                assert!(read_reply(&mut stream).await.starts_with(b":"));
            })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }

    let mut reader = connect(addr).await;
    send(&mut reader, &[b"LRANGE", b"shared-list", b"0", b"-1"]).await;
    let reply = read_reply(&mut reader).await;
    let reply_str = String::from_utf8_lossy(&reply);
    let item_count = (0..pusher_count)
        .filter(|i| reply_str.contains(&format!("item-{i:03}")))
        .count();
    assert_eq!(item_count, pusher_count, "expected every pushed item to survive");
}

// ==== stage 7-specific: scale that would be impractical as OS threads ===

/// The actual point of this stage, made concrete: 300 concurrent clients
/// as 300 spawned OS threads (stage 2-6's model) is 300 OS stacks (a few
/// hundred KB to a few MB reserved each) and 300 entries for the OS
/// scheduler to context-switch between. As 300 `tokio` tasks sharing a
/// handful of OS threads, it's unremarkable — which is exactly why this
/// test uses a count well past what the earlier stages' equivalent tests
/// used.
#[tokio::test(flavor = "multi_thread")]
async fn many_more_concurrent_clients_than_would_be_practical_as_os_threads() {
    let addr = spawn_server().await;
    let client_count = 300;

    let handles: Vec<_> = (0..client_count)
        .map(|i| {
            tokio::spawn(async move {
                let mut stream = connect(addr).await;
                let key = format!("scale-{i}");
                send(&mut stream, &[b"SET", key.as_bytes(), b"x"]).await;
                assert_eq!(read_reply(&mut stream).await, b"+OK\r\n");
                send(&mut stream, &[b"GET", key.as_bytes()]).await;
                assert_eq!(read_reply(&mut stream).await, b"$1\r\nx\r\n");
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}
