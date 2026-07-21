//! Turning a parsed RESP array into a [`Command`] (arity/known-command
//! validation) and a `Command` into a [`Reply`] (execution against the
//! shared [`Store`]). The pipeline shape — bytes -> args -> Command ->
//! Reply — is unchanged from stage 3; `execute` just does real work now.

use crate::resp::{Bytes, ParseResult, Reply, parse_multibulk};
use crate::store::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(String),
    Set(String, Bytes),
    Del(Vec<String>),
    Unknown(Bytes),
    WrongArity(Bytes),
}

/// Keys are modeled as `String`, so a non-UTF-8 key argument is
/// lossily converted rather than rejected outright — a deliberate
/// simplification (real Redis keys are fully binary-safe) that keeps
/// the store's `HashMap<String, Bytes>` shape simple for this stage.
fn key_from_bytes(bytes: Bytes) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
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
        "GET" => match args.len() {
            1 => Command::Get(key_from_bytes(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        "SET" => match args.len() {
            2 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                let value = it.next().unwrap();
                Command::Set(key, value)
            }
            _ => Command::WrongArity(name),
        },
        "DEL" => {
            if args.is_empty() {
                Command::WrongArity(name)
            } else {
                Command::Del(args.into_iter().map(key_from_bytes).collect())
            }
        }
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

pub fn execute(command: &Command, store: &Store) -> Reply {
    match command {
        Command::Ping(None) => Reply::Simple("PONG".to_string()),
        Command::Ping(Some(msg)) => Reply::Bulk(Some(msg.clone())),
        Command::Echo(msg) => Reply::Bulk(Some(msg.clone())),
        Command::Get(key) => Reply::Bulk(store.get(key)),
        Command::Set(key, value) => {
            store.set(key.clone(), value.clone());
            Reply::Simple("OK".to_string())
        }
        Command::Del(keys) => Reply::Integer(store.del(keys) as i64),
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
    fn ping_and_echo_are_unchanged_from_stage_3() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Simple("PONG".into()));

        let (cmd, _) = read_one(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Bulk(Some(b"hi".to_vec())));
    }

    #[test]
    fn get_on_a_missing_key_replies_a_null_bulk_string() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(cmd, Command::Get("foo".to_string()));
        assert_eq!(execute(&cmd, &store), Reply::Bulk(None));
    }

    #[test]
    fn set_then_get_round_trips_through_the_store() {
        let store = Store::new();
        let (set_cmd, _) = read_one(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            set_cmd,
            Command::Set("foo".to_string(), b"bar".to_vec())
        );
        assert_eq!(execute(&set_cmd, &store), Reply::Simple("OK".into()));

        let (get_cmd, _) = read_one(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(execute(&get_cmd, &store), Reply::Bulk(Some(b"bar".to_vec())));
    }

    #[test]
    fn del_reports_how_many_of_the_given_keys_existed() {
        let store = Store::new();
        store.set("a".to_string(), b"1".to_vec());

        let (cmd, _) = read_one(b"*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(cmd, Command::Del(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));
        assert_eq!(store.get("a"), None);
    }

    #[test]
    fn del_accepts_a_single_key_too() {
        let (cmd, _) = read_one(b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n");
        assert_eq!(cmd, Command::Del(vec!["a".to_string()]));
    }

    #[test]
    fn get_and_set_and_del_reject_the_wrong_number_of_arguments() {
        let (cmd, _) = read_one(b"*1\r\n$3\r\nGET\r\n");
        assert_eq!(cmd, Command::WrongArity(b"GET".to_vec()));

        let (cmd, _) = read_one(b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n");
        assert_eq!(cmd, Command::WrongArity(b"SET".to_vec()));

        let (cmd, _) = read_one(b"*1\r\n$3\r\nDEL\r\n");
        assert_eq!(cmd, Command::WrongArity(b"DEL".to_vec()));
    }

    #[test]
    fn unrecognized_command_name() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*1\r\n$3\r\nFOO\r\n");
        assert_eq!(cmd, Command::Unknown(b"FOO".to_vec()));
        let Reply::Error(msg) = execute(&cmd, &store) else {
            panic!("expected an error reply");
        };
        assert!(msg.contains("unknown command"));
        assert!(msg.contains("FOO"));
    }

    #[test]
    fn command_and_key_names_are_case_handled_independently() {
        // Command names are case-insensitive; key names are not touched.
        let (cmd, _) = read_one(b"*2\r\n$3\r\nget\r\n$3\r\nFoo\r\n");
        assert_eq!(cmd, Command::Get("Foo".to_string()));
    }
}
