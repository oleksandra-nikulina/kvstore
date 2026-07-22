use kvstore_stage3::run;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

fn spawn_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || run(listener).unwrap());
    addr
}

fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
}

/// Reads exactly `n` bytes, looping over `read()` since a reply can
/// legitimately arrive across more than one TCP segment.
fn read_n(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).unwrap();
    buf
}

#[test]
fn ping_with_no_argument_replies_pong() {
    let mut stream = connect(spawn_server());
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    assert_eq!(read_n(&mut stream, b"+PONG\r\n".len()), b"+PONG\r\n");
}

#[test]
fn ping_with_a_message_echoes_it_as_a_bulk_string() {
    let mut stream = connect(spawn_server());
    stream
        .write_all(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n")
        .unwrap();
    let expected = b"$5\r\nhello\r\n";
    assert_eq!(read_n(&mut stream, expected.len()), expected);
}

#[test]
fn echo_replies_with_the_argument_as_a_bulk_string() {
    let mut stream = connect(spawn_server());
    stream
        .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n")
        .unwrap();
    let expected = b"$5\r\nworld\r\n";
    assert_eq!(read_n(&mut stream, expected.len()), expected);
}

#[test]
fn unknown_command_gets_an_error_reply_but_the_connection_stays_open() {
    let mut stream = connect(spawn_server());
    stream.write_all(b"*1\r\n$3\r\nFOO\r\n").unwrap();

    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).unwrap();
    let reply = String::from_utf8_lossy(&buf[..n]);
    assert!(
        reply.starts_with('-'),
        "expected an error reply, got {reply:?}"
    );
    assert!(reply.contains("unknown command"));

    // Framing was still valid RESP, so the connection is still usable.
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    assert_eq!(read_n(&mut stream, b"+PONG\r\n".len()), b"+PONG\r\n");
}

#[test]
fn pipelined_commands_sent_in_one_write_each_get_a_reply() {
    let mut stream = connect(spawn_server());
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n")
        .unwrap();

    assert_eq!(read_n(&mut stream, b"+PONG\r\n".len()), b"+PONG\r\n");
    let expected = b"$2\r\nhi\r\n";
    assert_eq!(read_n(&mut stream, expected.len()), expected);
}

#[test]
fn a_command_split_across_multiple_writes_is_still_parsed_correctly() {
    let mut stream = connect(spawn_server());
    let full = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";

    // Trickle it in a few bytes at a time with small pauses, simulating
    // a slow client or a command that straddles TCP segments.
    for chunk in full.chunks(3) {
        stream.write_all(chunk).unwrap();
        thread::sleep(Duration::from_millis(5));
    }

    let expected = b"$5\r\nhello\r\n";
    assert_eq!(read_n(&mut stream, expected.len()), expected);
}

#[test]
fn a_bulk_string_payload_containing_crlf_round_trips_intact() {
    let mut stream = connect(spawn_server());
    // The payload "a\r\nb" is 4 bytes and contains an embedded CRLF; a
    // line-splitting parser would mishandle this.
    stream
        .write_all(b"*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n")
        .unwrap();
    let expected = b"$4\r\na\r\nb\r\n";
    assert_eq!(read_n(&mut stream, expected.len()), expected);
}

#[test]
fn malformed_input_gets_an_error_reply_and_the_connection_is_closed() {
    let mut stream = connect(spawn_server());
    stream.write_all(b"not resp at all\r\n").unwrap();

    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).unwrap();
    assert!(n > 0, "expected a protocol error reply");
    assert!(buf[0] == b'-', "expected an error reply");

    // The server closes the connection after a framing error — further
    // reads should observe EOF (a 0-length read) rather than hang.
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, 0, "expected the connection to be closed");
}
