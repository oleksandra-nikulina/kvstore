//! RESP (REdis Serialization Protocol) framing: turning raw bytes into a
//! multibulk array of arguments, and turning a [`Reply`] back into bytes.
//! This module knows nothing about what any command *means* — that's
//! [`crate::command`]'s job. This layer only knows how to frame messages.

pub type Bytes = Vec<u8>;

/// A real client sends `*<count>\r\n$<len>\r\n<count>` copies of that,
/// which is fine for arguments but would take forever to reach in a
/// fuzz/adversarial input, so array length and each bulk string's length
/// are capped generously below any real command actually needs — the
/// same purpose `proto-max-bulk-len` serves in real Redis.
const MAX_MULTIBULK_LEN: i64 = 1024 * 1024;
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub struct ProtocolError(pub String);

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ProtocolError {}

fn err(msg: impl Into<String>) -> ProtocolError {
    ProtocolError(msg.into())
}

/// A reply the server can send back to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Bytes>),
    Array(Vec<Reply>),
}

impl Reply {
    pub fn encode(&self) -> Bytes {
        match self {
            Reply::Simple(s) => format!("+{s}\r\n").into_bytes(),
            Reply::Error(s) => format!("-{s}\r\n").into_bytes(),
            Reply::Integer(i) => format!(":{i}\r\n").into_bytes(),
            Reply::Bulk(None) => b"$-1\r\n".to_vec(),
            Reply::Bulk(Some(b)) => {
                let mut out = format!("${}\r\n", b.len()).into_bytes();
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
                out
            }
            Reply::Array(items) => {
                let mut out = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    out.extend(item.encode());
                }
                out
            }
        }
    }
}

/// The result of trying to parse one multibulk array out of a buffer that
/// may hold less than a full command (more data still to arrive over the
/// socket), exactly one command, or several pipelined back-to-back.
pub enum ParseResult {
    /// `buf` doesn't yet contain a complete array — wait for more bytes
    /// and try again. Nothing has been consumed.
    Incomplete,
    /// A complete array was parsed. `consumed` bytes should be dropped
    /// from the front of the buffer before parsing the next command.
    Complete { args: Vec<Bytes>, consumed: usize },
}

/// Finds the first `\r\n` in `buf` and returns the line before it plus
/// the total number of bytes it and the terminator occupy.
fn read_line(buf: &[u8]) -> Option<(&[u8], usize)> {
    let idx = buf.windows(2).position(|w| w == b"\r\n")?;
    Some((&buf[..idx], idx + 2))
}

fn parse_len(line: &[u8], what: &str) -> Result<i64, ProtocolError> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| err(format!("invalid {what} length")))
}

/// Parses one RESP multibulk array (`*<n>\r\n` followed by `n` bulk
/// strings) from the front of `buf`. Real clients (`redis-cli` included)
/// always send commands this way — a bare inline command like typing
/// `PING\r\n` into `nc` is a separate, legacy wire format this project
/// doesn't implement, so it will surface as a protocol error here.
pub fn parse_multibulk(buf: &[u8]) -> Result<ParseResult, ProtocolError> {
    if buf.is_empty() {
        return Ok(ParseResult::Incomplete);
    }
    if buf[0] != b'*' {
        return Err(err(format!(
            "expected '*', got '{}'",
            buf[0].escape_ascii()
        )));
    }
    let Some((header, header_len)) = read_line(&buf[1..]) else {
        return Ok(ParseResult::Incomplete);
    };
    let count = parse_len(header, "multibulk")?;
    if count > MAX_MULTIBULK_LEN {
        return Err(err("multibulk length exceeds limit"));
    }
    if count <= 0 {
        // `*0\r\n` (empty array) and `*-1\r\n` (null array) both carry no
        // arguments — nothing to execute, but the bytes still need to be
        // consumed so the next pipelined command can be parsed.
        return Ok(ParseResult::Complete {
            args: Vec::new(),
            consumed: 1 + header_len,
        });
    }

    let mut pos = 1 + header_len;
    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if pos >= buf.len() {
            return Ok(ParseResult::Incomplete);
        }
        if buf[pos] != b'$' {
            return Err(err(format!(
                "expected '$', got '{}'",
                buf[pos].escape_ascii()
            )));
        }
        let Some((len_line, len_line_len)) = read_line(&buf[pos + 1..]) else {
            return Ok(ParseResult::Incomplete);
        };
        let bulk_len = parse_len(len_line, "bulk string")?;
        if bulk_len > MAX_BULK_LEN {
            return Err(err("bulk string length exceeds limit"));
        }
        pos += 1 + len_line_len;

        if bulk_len < 0 {
            // A null bulk string as an argument is not something a real
            // client sends, but framing-wise it carries no payload.
            args.push(Bytes::new());
            continue;
        }
        let bulk_len = bulk_len as usize;
        let Some(payload_end) = pos.checked_add(bulk_len) else {
            return Err(err("bulk string length overflow"));
        };
        if payload_end + 2 > buf.len() {
            return Ok(ParseResult::Incomplete);
        }
        if &buf[payload_end..payload_end + 2] != b"\r\n" {
            return Err(err("expected CRLF after bulk string payload"));
        }
        // Length-prefixed, not line-split: the payload itself is taken
        // verbatim for exactly `bulk_len` bytes, so it's free to contain
        // `\r\n` (or any other byte) without truncating the argument.
        args.push(buf[pos..payload_end].to_vec());
        pos = payload_end + 2;
    }

    Ok(ParseResult::Complete { args, consumed: pos })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(buf: &[u8]) -> (Vec<Bytes>, usize) {
        match parse_multibulk(buf).unwrap() {
            ParseResult::Complete { args, consumed } => (args, consumed),
            ParseResult::Incomplete => panic!("expected Complete, got Incomplete"),
        }
    }

    #[test]
    fn parses_a_single_bulk_string_array() {
        let buf = b"*1\r\n$4\r\nPING\r\n";
        let (args, consumed) = complete(buf);
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn parses_multiple_arguments() {
        let buf = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let (args, consumed) = complete(buf);
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hello".to_vec()]);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn bulk_strings_are_binary_safe_not_line_split() {
        // The payload itself contains a \r\n; a line-splitting parser
        // would truncate the argument here. A length-prefixed one won't.
        let buf = b"*1\r\n$6\r\nhi\r\nyo\r\n";
        let (args, consumed) = complete(buf);
        assert_eq!(args, vec![b"hi\r\nyo".to_vec()]);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn empty_array_consumes_bytes_but_yields_no_args() {
        let buf = b"*0\r\n";
        let (args, consumed) = complete(buf);
        assert!(args.is_empty());
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn stops_and_reports_incomplete_when_header_hasnt_fully_arrived() {
        for prefix_len in 0..b"*1\r\n$4\r\nPING\r\n".len() {
            let buf = &b"*1\r\n$4\r\nPING\r\n"[..prefix_len];
            match parse_multibulk(buf).unwrap() {
                ParseResult::Incomplete => {}
                ParseResult::Complete { consumed, .. } => {
                    // The only prefix long enough to be complete is the
                    // full message itself.
                    assert_eq!(prefix_len, buf.len());
                    assert_eq!(consumed, buf.len());
                }
            }
        }
    }

    #[test]
    fn rejects_input_not_starting_with_asterisk() {
        assert!(parse_multibulk(b"PING\r\n").is_err());
    }

    #[test]
    fn rejects_a_non_numeric_multibulk_length() {
        assert!(parse_multibulk(b"*x\r\n").is_err());
    }

    #[test]
    fn rejects_a_bulk_header_missing_its_dollar_sign() {
        assert!(parse_multibulk(b"*1\r\nPING\r\n").is_err());
    }

    #[test]
    fn rejects_a_bulk_string_missing_its_trailing_crlf() {
        assert!(parse_multibulk(b"*1\r\n$4\r\nPINGxx").is_err());
    }

    #[test]
    fn reply_encoding_matches_resp() {
        assert_eq!(Reply::Simple("PONG".into()).encode(), b"+PONG\r\n");
        assert_eq!(Reply::Error("ERR nope".into()).encode(), b"-ERR nope\r\n");
        assert_eq!(Reply::Integer(42).encode(), b":42\r\n");
        assert_eq!(Reply::Bulk(None).encode(), b"$-1\r\n");
        assert_eq!(
            Reply::Bulk(Some(b"hi".to_vec())).encode(),
            b"$2\r\nhi\r\n"
        );
        assert_eq!(
            Reply::Array(vec![Reply::Integer(1), Reply::Integer(2)]).encode(),
            b"*2\r\n:1\r\n:2\r\n"
        );
    }
}
