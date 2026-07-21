//! Turning a parsed RESP array into a [`Command`] (arity/known-command
//! validation) and a `Command` into a [`Reply`] (execution). No storage
//! yet — that's stage 4 — so execution here is trivial, but the pipeline
//! shape (bytes -> args -> Command -> Reply) is the one every later
//! stage's command set slots into.

use crate::resp::{Bytes, ParseResult, Reply, parse_multibulk};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Unknown(Bytes),
    WrongArity(Bytes),
}

fn command_from_args(mut args: Vec<Bytes>) -> Command {
    let name = args.remove(0);
    let name_upper = String::from_utf8_lossy(&name).to_ascii_uppercase();
    match name_upper.as_str() {
        "PING" => match args.len() {
            0 => Command::Ping(None),
            1 => Command::Ping(Some(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        "ECHO" => match args.len() {
            1 => Command::Echo(args.into_iter().next().unwrap()),
            _ => Command::WrongArity(name),
        },
        _ => Command::Unknown(name),
    }
}

/// What came out of trying to read one command from the front of a
/// connection's buffer.
pub enum ReadResult {
    /// Not enough bytes buffered yet — read more from the socket.
    Incomplete,
    /// `consumed` bytes were a complete but empty RESP array (`*0\r\n`);
    /// drop them and keep parsing, no reply is sent for these.
    Empty { consumed: usize },
    /// A complete command was parsed; `consumed` bytes should be dropped
    /// from the buffer once it's been executed.
    Command { command: Command, consumed: usize },
}

pub use crate::resp::ProtocolError;

/// Reads one command from the front of `buf`, if a full one is there.
pub fn read_command(buf: &[u8]) -> Result<ReadResult, ProtocolError> {
    match parse_multibulk(buf)? {
        ParseResult::Incomplete => Ok(ReadResult::Incomplete),
        ParseResult::Complete { args, consumed } if args.is_empty() => {
            Ok(ReadResult::Empty { consumed })
        }
        ParseResult::Complete { args, consumed } => Ok(ReadResult::Command {
            command: command_from_args(args),
            consumed,
        }),
    }
}

pub fn execute(command: &Command) -> Reply {
    match command {
        Command::Ping(None) => Reply::Simple("PONG".to_string()),
        Command::Ping(Some(msg)) => Reply::Bulk(Some(msg.clone())),
        Command::Echo(msg) => Reply::Bulk(Some(msg.clone())),
        Command::Unknown(name) => Reply::Error(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(name)
        )),
        Command::WrongArity(name) => Reply::Error(format!(
            "ERR wrong number of arguments for '{}' command",
            String::from_utf8_lossy(name).to_lowercase()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_one(buf: &[u8]) -> (Command, usize) {
        match read_command(buf).unwrap() {
            ReadResult::Command { command, consumed } => (command, consumed),
            ReadResult::Incomplete => panic!("expected a command, got Incomplete"),
            ReadResult::Empty { .. } => panic!("expected a command, got Empty"),
        }
    }

    #[test]
    fn ping_with_no_argument() {
        let (cmd, _) = read_one(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(cmd, Command::Ping(None));
        assert_eq!(execute(&cmd), Reply::Simple("PONG".into()));
    }

    #[test]
    fn ping_with_a_message_argument() {
        let (cmd, _) = read_one(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n");
        assert_eq!(cmd, Command::Ping(Some(b"hello".to_vec())));
        assert_eq!(execute(&cmd), Reply::Bulk(Some(b"hello".to_vec())));
    }

    #[test]
    fn ping_with_too_many_arguments_is_a_wrong_arity_error() {
        let (cmd, _) = read_one(b"*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(cmd, Command::WrongArity(b"PING".to_vec()));
        let Reply::Error(msg) = execute(&cmd) else {
            panic!("expected an error reply");
        };
        assert!(msg.contains("wrong number of arguments"));
    }

    #[test]
    fn echo_requires_exactly_one_argument() {
        let (cmd, _) = read_one(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        assert_eq!(cmd, Command::Echo(b"hi".to_vec()));
        assert_eq!(execute(&cmd), Reply::Bulk(Some(b"hi".to_vec())));

        let (cmd, _) = read_one(b"*1\r\n$4\r\nECHO\r\n");
        assert_eq!(cmd, Command::WrongArity(b"ECHO".to_vec()));
    }

    #[test]
    fn command_names_are_case_insensitive() {
        let (cmd, _) = read_one(b"*1\r\n$4\r\nping\r\n");
        assert_eq!(cmd, Command::Ping(None));
        let (cmd, _) = read_one(b"*1\r\n$4\r\nPiNg\r\n");
        assert_eq!(cmd, Command::Ping(None));
    }

    #[test]
    fn unrecognized_command_name() {
        let (cmd, _) = read_one(b"*1\r\n$3\r\nFOO\r\n");
        assert_eq!(cmd, Command::Unknown(b"FOO".to_vec()));
        let Reply::Error(msg) = execute(&cmd) else {
            panic!("expected an error reply");
        };
        assert!(msg.contains("unknown command"));
        assert!(msg.contains("FOO"));
    }

    #[test]
    fn empty_array_yields_no_command() {
        match read_command(b"*0\r\n").unwrap() {
            ReadResult::Empty { consumed } => assert_eq!(consumed, 4),
            _ => panic!("expected Empty"),
        }
    }

    #[test]
    fn incomplete_input_yields_incomplete() {
        match read_command(b"*1\r\n$4\r\nPI").unwrap() {
            ReadResult::Incomplete => {}
            _ => panic!("expected Incomplete"),
        }
    }

    #[test]
    fn malformed_input_is_a_protocol_error() {
        assert!(read_command(b"not resp at all").is_err());
    }
}
