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
    Lpush(String, Vec<Bytes>),
    Rpush(String, Vec<Bytes>),
    Lrange(String, i64, i64),
    Lpop(String),
    Hset(String, String, Bytes),
    Hget(String, String),
    Hgetall(String),
    Hdel(String, Vec<String>),
    Sadd(String, Vec<Bytes>),
    Srem(String, Vec<Bytes>),
    Smembers(String),
    Sismember(String, Bytes),
    Unknown(Bytes),
    WrongArity(Bytes),
    NotAnInteger,
}

fn key_from_bytes(bytes: Bytes) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
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
                match parse_i64(&it.next().unwrap()) {
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
                match parse_i64(&it.next().unwrap()) {
                    Some(millis) => Command::Pexpire(key, Duration::from_millis(millis.max(0) as u64)),
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
        "LPUSH" => {
            if args.len() < 2 {
                Command::WrongArity(name)
            } else {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                Command::Lpush(key, it.collect())
            }
        }
        "RPUSH" => {
            if args.len() < 2 {
                Command::WrongArity(name)
            } else {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                Command::Rpush(key, it.collect())
            }
        }
        "LRANGE" => match args.len() {
            3 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                let start = parse_i64(&it.next().unwrap());
                let stop = parse_i64(&it.next().unwrap());
                match (start, stop) {
                    (Some(start), Some(stop)) => Command::Lrange(key, start, stop),
                    _ => Command::NotAnInteger,
                }
            }
            _ => Command::WrongArity(name),
        },
        "LPOP" => match args.len() {
            1 => Command::Lpop(key_from_bytes(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        "HSET" => match args.len() {
            3 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                let field = key_from_bytes(it.next().unwrap());
                let value = it.next().unwrap();
                Command::Hset(key, field, value)
            }
            _ => Command::WrongArity(name),
        },
        "HGET" => match args.len() {
            2 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                let field = key_from_bytes(it.next().unwrap());
                Command::Hget(key, field)
            }
            _ => Command::WrongArity(name),
        },
        "HGETALL" => match args.len() {
            1 => Command::Hgetall(key_from_bytes(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        "HDEL" => {
            if args.len() < 2 {
                Command::WrongArity(name)
            } else {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                Command::Hdel(key, it.map(key_from_bytes).collect())
            }
        }
        "SADD" => {
            if args.len() < 2 {
                Command::WrongArity(name)
            } else {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                Command::Sadd(key, it.collect())
            }
        }
        "SREM" => {
            if args.len() < 2 {
                Command::WrongArity(name)
            } else {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                Command::Srem(key, it.collect())
            }
        }
        "SMEMBERS" => match args.len() {
            1 => Command::Smembers(key_from_bytes(args.into_iter().next().unwrap())),
            _ => Command::WrongArity(name),
        },
        "SISMEMBER" => match args.len() {
            2 => {
                let mut it = args.into_iter();
                let key = key_from_bytes(it.next().unwrap());
                let member = it.next().unwrap();
                Command::Sismember(key, member)
            }
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

fn ceil_seconds(d: Duration) -> i64 {
    d.as_millis().div_ceil(1000) as i64
}

/// `command_name` only shapes the error message, matching real Redis's
/// "invalid expire time in 'expire'/'pexpire' command".
fn expire_reply(store: &Store, key: &str, ttl: Duration, command_name: &str) -> Reply {
    match store.expire(key, ttl) {
        ExpireResult::Set => Reply::Integer(1),
        ExpireResult::Missing => Reply::Integer(0),
        ExpireResult::Overflow => Reply::Error(format!(
            "ERR invalid expire time in '{command_name}' command"
        )),
    }
}

fn wrongtype() -> Reply {
    Reply::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

fn bulk_array(items: Vec<Bytes>) -> Reply {
    Reply::Array(items.into_iter().map(|b| Reply::Bulk(Some(b))).collect())
}

pub fn execute(command: &Command, store: &Store) -> Reply {
    match command {
        Command::Ping(None) => Reply::Simple("PONG".to_string()),
        Command::Ping(Some(msg)) => Reply::Bulk(Some(msg.clone())),
        Command::Echo(msg) => Reply::Bulk(Some(msg.clone())),
        Command::Get(key) => match store.get(key) {
            Ok(value) => Reply::Bulk(value),
            Err(_) => wrongtype(),
        },
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
        Command::Lpush(key, values) => match store.lpush(key, values) {
            Ok(len) => Reply::Integer(len as i64),
            Err(_) => wrongtype(),
        },
        Command::Rpush(key, values) => match store.rpush(key, values) {
            Ok(len) => Reply::Integer(len as i64),
            Err(_) => wrongtype(),
        },
        Command::Lrange(key, start, stop) => match store.lrange(key, *start, *stop) {
            Ok(items) => bulk_array(items),
            Err(_) => wrongtype(),
        },
        Command::Lpop(key) => match store.lpop(key) {
            Ok(value) => Reply::Bulk(value),
            Err(_) => wrongtype(),
        },
        Command::Hset(key, field, value) => {
            match store.hset(key, field.clone(), value.clone()) {
                Ok(is_new) => Reply::Integer(if is_new { 1 } else { 0 }),
                Err(_) => wrongtype(),
            }
        }
        Command::Hget(key, field) => match store.hget(key, field) {
            Ok(value) => Reply::Bulk(value),
            Err(_) => wrongtype(),
        },
        Command::Hgetall(key) => match store.hgetall(key) {
            Ok(pairs) => Reply::Array(
                pairs
                    .into_iter()
                    .flat_map(|(f, v)| [Reply::Bulk(Some(f.into_bytes())), Reply::Bulk(Some(v))])
                    .collect(),
            ),
            Err(_) => wrongtype(),
        },
        Command::Hdel(key, fields) => match store.hdel(key, fields) {
            Ok(n) => Reply::Integer(n as i64),
            Err(_) => wrongtype(),
        },
        Command::Sadd(key, members) => match store.sadd(key, members) {
            Ok(n) => Reply::Integer(n as i64),
            Err(_) => wrongtype(),
        },
        Command::Srem(key, members) => match store.srem(key, members) {
            Ok(n) => Reply::Integer(n as i64),
            Err(_) => wrongtype(),
        },
        Command::Smembers(key) => match store.smembers(key) {
            Ok(members) => bulk_array(members),
            Err(_) => wrongtype(),
        },
        Command::Sismember(key, member) => match store.sismember(key, member) {
            Ok(is_member) => Reply::Integer(if is_member { 1 } else { 0 }),
            Err(_) => wrongtype(),
        },
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
    fn lpush_then_lrange_round_trips_through_the_store() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*4\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(
            cmd,
            Command::Lpush("l".to_string(), vec![b"a".to_vec(), b"b".to_vec()])
        );
        assert_eq!(execute(&cmd, &store), Reply::Integer(2));

        let (cmd, _) = read_one(b"*4\r\n$6\r\nLRANGE\r\n$1\r\nl\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(cmd, Command::Lrange("l".to_string(), 0, -1));
        assert_eq!(
            execute(&cmd, &store),
            Reply::Array(vec![
                Reply::Bulk(Some(b"b".to_vec())),
                Reply::Bulk(Some(b"a".to_vec())),
            ])
        );
    }

    #[test]
    fn lpop_replies_a_null_bulk_string_once_the_list_is_empty() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n");
        execute(&cmd, &store);

        let (cmd, _) = read_one(b"*2\r\n$4\r\nLPOP\r\n$1\r\nl\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Bulk(Some(b"a".to_vec())));
        assert_eq!(execute(&cmd, &store), Reply::Bulk(None));
    }

    #[test]
    fn hset_hget_round_trip() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n");
        assert_eq!(
            cmd,
            Command::Hset("h".to_string(), "f".to_string(), b"v".to_vec())
        );
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (cmd, _) = read_one(b"*3\r\n$4\r\nHGET\r\n$1\r\nh\r\n$1\r\nf\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Bulk(Some(b"v".to_vec())));
    }

    #[test]
    fn hgetall_returns_a_flat_field_value_array() {
        let store = Store::new();
        store.hset("h", "f".to_string(), b"v".to_vec()).unwrap();

        let (cmd, _) = read_one(b"*2\r\n$7\r\nHGETALL\r\n$1\r\nh\r\n");
        assert_eq!(
            execute(&cmd, &store),
            Reply::Array(vec![
                Reply::Bulk(Some(b"f".to_vec())),
                Reply::Bulk(Some(b"v".to_vec())),
            ])
        );
    }

    #[test]
    fn sadd_srem_smembers_sismember_round_trip() {
        let store = Store::new();
        let (cmd, _) = read_one(b"*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\na\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (cmd, _) = read_one(b"*3\r\n$9\r\nSISMEMBER\r\n$1\r\ns\r\n$1\r\na\r\n");
        assert_eq!(cmd, Command::Sismember("s".to_string(), b"a".to_vec()));
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (cmd, _) = read_one(b"*3\r\n$4\r\nSREM\r\n$1\r\ns\r\n$1\r\na\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Integer(1));

        let (cmd, _) = read_one(b"*2\r\n$8\r\nSMEMBERS\r\n$1\r\ns\r\n");
        assert_eq!(execute(&cmd, &store), Reply::Array(vec![]));
    }

    #[test]
    fn wrongtype_error_when_a_list_command_hits_a_string_key() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        let (cmd, _) = read_one(b"*3\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n$1\r\na\r\n");
        let Reply::Error(msg) = execute(&cmd, &store) else {
            panic!("expected an error reply");
        };
        assert!(msg.starts_with("WRONGTYPE"));
    }

    #[test]
    fn non_integer_lrange_bounds_are_rejected() {
        let (cmd, _) = read_one(b"*4\r\n$6\r\nLRANGE\r\n$1\r\nl\r\n$3\r\nabc\r\n$2\r\n-1\r\n");
        assert_eq!(cmd, Command::NotAnInteger);
    }

    #[test]
    fn list_hash_set_commands_reject_the_wrong_number_of_arguments() {
        let (cmd, _) = read_one(b"*2\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n");
        assert_eq!(cmd, Command::WrongArity(b"LPUSH".to_vec()));

        let (cmd, _) = read_one(b"*3\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n");
        assert_eq!(cmd, Command::WrongArity(b"HSET".to_vec()));

        let (cmd, _) = read_one(b"*2\r\n$4\r\nSADD\r\n$1\r\ns\r\n");
        assert_eq!(cmd, Command::WrongArity(b"SADD".to_vec()));
    }
}
