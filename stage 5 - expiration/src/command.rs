//! Turning a parsed RESP array into a [`Command`] (arity/known-command
//! validation) and a `Command` into a [`Reply`] (execution against the
//! shared [`Store`]).

use crate::resp::{Bytes, ParseResult, Reply, parse_multibulk};
use crate::store::{ExpireResult, Store};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(String),
    Set(String, Bytes),
    Del(Vec<String>),
    Expire(String, Duration),
    Pexpire(String, Duration),
    Ttl(String),
    Persist(String),
    Unknown(Bytes),
    WrongArity(Bytes),
    /// A numeric argument (`EXPIRE`/`PEXPIRE`'s TTL) wasn't a valid
    /// integer — a distinct case from an unknown command or bad arity.
    NotAnInteger,
}

/// Keys are modeled as `String`, so a non-UTF-8 key argument is lossily
/// converted rather than rejected outright — the same simplification
/// stage 4 made, unchanged here.
fn key_from_bytes(bytes: Bytes) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}

/// A negative TTL argument is clamped to zero rather than rejected —
/// real Redis treats a negative `EXPIRE`/`PEXPIRE` as "expire right
/// now," and `Duration::ZERO` already means exactly that to `Store`, so
/// no separate code path is needed for it.
fn parse_ttl_arg(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
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
        "EXPIRE" => match args.len() {
            2 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                match parse_ttl_arg(&it.next().unwrap()) {
                    Some(seconds) => Command::Expire(key, Duration::from_secs(seconds.max(0) as u64)),
                    None => Command::NotAnInteger,
                }
            }
            _ => Command::WrongArity(name),
        },
        "PEXPIRE" => match args.len() {
            2 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                match parse_ttl_arg(&it.next().unwrap()) {
                    Some(millis) => {
                        Command::Pexpire(key, Duration::from_millis(millis.max(0) as u64))
                    }
                    None => Command::NotAnInteger,
                }
            }
            _ => Command::WrongArity(name),
        },
        "TTL" => match args.len() {
            1 => Command::Ttl(key_from_bytes(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        "PERSIST" => match args.len() {
            1 => Command::Persist(key_from_bytes(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        _ => Command::Unknown(name),
    }
}

pub enum ReadResult {
    Incomplete,
    Empty { consumed: usize },
    Command { command: Command, consumed: usize },
}

pub use crate::resp::ProtocolError;

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

/// Rounds a remaining TTL up to whole seconds, so a key that's 4999ms
/// from expiring reports `TTL` as 5, not 4 — matching real Redis, and
/// avoiding a key that looks expired to a client the instant after it
/// was set with a round number of seconds.
fn ceil_seconds(d: Duration) -> i64 {
    d.as_millis().div_ceil(1000) as i64
}

/// `command_name` is only used to shape the error message, matching
/// real Redis's "invalid expire time in 'expire'/'pexpire' command".
fn expire_reply(store: &Store, key: &str, ttl: Duration, command_name: &str) -> Reply {
    match store.expire(key, ttl) {
        ExpireResult::Set => Reply::Integer(1),
        ExpireResult::Missing => Reply::Integer(0),
        ExpireResult::Overflow => Reply::Error(format!(
            "ERR invalid expire time in '{command_name}' command"
        )),
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
        Command::Expire(key, ttl) => expire_reply(store, key, *ttl, "expire"),
        Command::Pexpire(key, ttl) => expire_reply(store, key, *ttl, "pexpire"),
        Command::Ttl(key) => match store.ttl(key) {
            None => Reply::Integer(-2),
            Some(None) => Reply::Integer(-1),
            Some(Some(remaining)) => Reply::Integer(ceil_seconds(remaining)),
        },
        Command::Persist(key) => Reply::Integer(if store.persist(key) { 1 } else { 0 }),
        Command::NotAnInteger => {
            Reply::Error("ERR value is not an integer or out of range".to_string())
        }
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
    fn get_set_del_are_unchanged_from_stage_4() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Simple("OK".into()));
        let (cmd, _) = read_one(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Bulk(Some(b"v".to_vec())));
    }

    #[test]
    fn expire_parses_seconds_and_sets_a_ttl() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        let (cmd, _) = read_one(b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n60\r\n");
        assert_eq!(
            cmd,
            Command::Expire("k".to_string(), Duration::from_secs(60))
        );
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (ttl_cmd, _) = read_one(b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
        assert_eq!(execute(&ttl_cmd, &store), Reply::Integer(60));
    }

    #[test]
    fn expire_on_a_missing_key_replies_zero() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n60\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Integer(0));
    }

    #[test]
    fn pexpire_parses_milliseconds() {
        let (cmd, _) = read_one(b"*3\r\n$7\r\nPEXPIRE\r\n$1\r\nk\r\n$4\r\n5000\r\n");
        assert_eq!(
            cmd,
            Command::Pexpire("k".to_string(), Duration::from_millis(5000))
        );
    }

    #[test]
    fn ttl_replies_minus_two_for_a_missing_key_and_minus_one_for_no_expiry() {
        let store = Store::new();
        let (missing, _) = read_one(b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
        assert_eq!(execute(&missing, &store), Reply::Integer(-2));

        store.set("k".to_string(), b"v".to_vec());
        let (no_ttl, _) = read_one(b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
        assert_eq!(execute(&no_ttl, &store), Reply::Integer(-1));
    }

    #[test]
    fn persist_clears_a_ttl_previously_set_by_expire() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_secs(60));

        let (cmd, _) = read_one(b"*2\r\n$7\r\nPERSIST\r\n$1\r\nk\r\n");
        assert_eq!(cmd, Command::Persist("k".to_string()));
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (ttl_cmd, _) = read_one(b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
        assert_eq!(execute(&ttl_cmd, &store), Reply::Integer(-1));
    }

    #[test]
    fn a_non_numeric_ttl_argument_is_rejected_before_touching_the_store() {
        let (cmd, _) = read_one(b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$3\r\nabc\r\n");
        assert_eq!(cmd, Command::NotAnInteger);
        let Reply::Error(msg) = execute(&cmd, &Store::new()) else {
            panic!("expected an error reply");
        };
        assert!(msg.contains("not an integer"));
    }

    #[test]
    fn a_negative_ttl_is_clamped_to_immediate_expiry() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        let (cmd, _) = read_one(b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n-1\r\n");
        assert_eq!(cmd, Command::Expire("k".to_string(), Duration::ZERO));
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (get_cmd, _) = read_one(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        assert_eq!(execute(&get_cmd, &store), Reply::Bulk(None));
    }

    #[test]
    fn setting_a_key_clears_its_previous_ttl() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_secs(60));

        let (set_cmd, _) = read_one(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv2\r\n");
        execute(&set_cmd, &store);

        let (ttl_cmd, _) = read_one(b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
        assert_eq!(execute(&ttl_cmd, &store), Reply::Integer(-1));
    }

    #[test]
    fn expire_pexpire_ttl_persist_reject_the_wrong_number_of_arguments() {
        let (cmd, _) = read_one(b"*2\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n");
        assert_eq!(cmd, Command::WrongArity(b"EXPIRE".to_vec()));

        let (cmd, _) = read_one(b"*1\r\n$3\r\nTTL\r\n");
        assert_eq!(cmd, Command::WrongArity(b"TTL".to_vec()));

        let (cmd, _) = read_one(b"*1\r\n$7\r\nPERSIST\r\n");
        assert_eq!(cmd, Command::WrongArity(b"PERSIST".to_vec()));
    }
}
