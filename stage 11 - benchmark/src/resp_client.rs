//! A minimal RESP client — just enough to drive a `SET`/`GET` workload
//! and correctly find reply boundaries, not a general parser. This is
//! deliberately much smaller than the server-side `resp.rs` from
//! earlier stages: a benchmark client only needs to know *when* a reply
//! finished arriving (to measure latency and stay in lockstep with the
//! server), never what it actually says. It also doesn't need to
//! understand arrays — `SET`/`GET` never reply with one — which is what
//! makes this safe to point at either this project's own servers *or*
//! real Redis: both speak RESP for these two commands identically.

use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// The type every call site actually holds — a buffered reader wrapping
/// the raw connection. `BufReader<TcpStream>` still implements
/// `AsyncWrite` (forwarded straight through to the inner stream,
/// unbuffered — writes don't need this), so `send_command` and
/// `read_reply` share the same handle without needing to split the
/// connection into separate read/write halves.
pub type Connection = BufReader<TcpStream>;

pub fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend(format!("${}\r\n", part.len()).into_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub async fn send_command(stream: &mut Connection, parts: &[&[u8]]) -> io::Result<()> {
    stream.write_all(&encode_command(parts)).await
}

/// `read_until` (not a manual byte-by-byte `read_exact` loop, which
/// this used to be) scans the buffered reader's already-filled buffer
/// for the delimiter in one pass, only reaching for another real read
/// once that buffer is empty — one syscall serves many reply lines
/// instead of one syscall per byte. Found via code review: at high
/// `--clients`/`--pipeline`, the benchmark client's own syscall
/// overhead could become the actual bottleneck being measured instead
/// of the server under test.
async fn read_line(stream: &mut Connection) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    stream.read_until(b'\n', &mut out).await?;
    if !out.ends_with(b"\r\n") {
        // Either the connection closed before a full line arrived, or
        // the server sent something that isn't RESP — either way this
        // isn't a reply we can trust the framing of.
        return Err(io::Error::other(
            "malformed reply: line not terminated with CRLF",
        ));
    }
    Ok(out)
}

/// Reads and fully consumes exactly one reply — a simple string, error,
/// integer, or bulk string (the only shapes `SET`/`GET` ever reply
/// with). The content is discarded; only correct framing matters here.
pub async fn read_reply(stream: &mut Connection) -> io::Result<()> {
    let line = read_line(stream).await?;
    if line[0] == b'$' {
        let len: i64 = std::str::from_utf8(&line[1..line.len() - 2])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::other("malformed bulk length in reply"))?;
        if len >= 0 {
            let mut payload = vec![0u8; len as usize + 2];
            stream.read_exact(&mut payload).await?;
        }
    }
    Ok(())
}
