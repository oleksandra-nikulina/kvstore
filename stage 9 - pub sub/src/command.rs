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
    /// One or more channels to subscribe to.
    Subscribe(Vec<String>),
    /// Channels to unsubscribe from — empty means "all currently
    /// subscribed," which is itself a valid, arity-0 form (unlike every
    /// other variadic command in this project, where zero arguments is
    /// `WrongArity`). Distinguishing "unsubscribe from nothing" from
    /// "unsubscribe from everything" needs the connection's live
    /// subscription set, which isn't available at parse time — so this
    /// variant only carries what the client actually typed, and
    /// `lib.rs::dispatch` resolves the empty case against that set.
    Unsubscribe(Vec<String>),
    Publish(String, Bytes),
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
                    Some(seconds) => {
                        Command::Expire(key, Duration::from_secs(seconds.max(0) as u64))
                    }
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
        "SUBSCRIBE" => {
            if args.is_empty() {
                Command::WrongArity(name)
            } else {
                Command::Subscribe(args.into_iter().map(key_from_bytes).collect())
            }
        }
        "UNSUBSCRIBE" => Command::Unsubscribe(args.into_iter().map(key_from_bytes).collect()),
        "PUBLISH" => match args.len() {
            2 => {
                let mut it = args.into_iter();
                let channel = key_from_bytes(it.next().unwrap());
                let message = it.next().unwrap();
                Command::Publish(channel, message)
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
        Command::Hset(key, field, value) => match store.hset(key, field.clone(), value.clone()) {
            Ok(is_new) => Reply::Integer(if is_new { 1 } else { 0 }),
            Err(_) => wrongtype(),
        },
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
        Command::Subscribe(_) | Command::Unsubscribe(_) | Command::Publish(..) => {
            unreachable!(
                "SUBSCRIBE/UNSUBSCRIBE/PUBLISH need the connection's live subscription set \
                 and the shared PubSub registry, neither of which this function has access \
                 to — lib.rs::dispatch matches on them before ever calling execute()"
            )
        }
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

/// The canonical RESP arguments to append to the AOF for this command,
/// or `None` if it never mutates the store and so never needs logging.
/// Reconstructed from the already-parsed `Command`, not the original
/// wire bytes — e.g. a lowercase `set` arrives on the wire but is always
/// logged as `SET`, and an `EXPIRE`'s seconds argument is rebuilt from
/// the `Duration` that was actually stored, not the client's original
/// digits (irrelevant here since seconds round-trip exactly, but the
/// same wouldn't be true if this stage ever needed fractional TTLs).
///
/// Known simplification, not fixed here: `EXPIRE`/`PEXPIRE` log a
/// *relative* TTL. Replaying them long after the fact re-arms the TTL
/// relative to replay time, not the original absolute expiry — real
/// Redis's AOF rewrites these to an absolute-timestamp command
/// specifically to avoid this. Out of scope for this stage; see the
/// stage README.
pub fn aof_args(command: &Command) -> Option<Vec<Bytes>> {
    fn key_bytes(key: &str) -> Bytes {
        key.as_bytes().to_vec()
    }

    match command {
        Command::Set(key, value) => Some(vec![b"SET".to_vec(), key_bytes(key), value.clone()]),
        Command::Del(keys) => {
            let mut args = vec![b"DEL".to_vec()];
            args.extend(keys.iter().map(|k| key_bytes(k)));
            Some(args)
        }
        Command::Expire(key, ttl) => Some(vec![
            b"EXPIRE".to_vec(),
            key_bytes(key),
            ttl.as_secs().to_string().into_bytes(),
        ]),
        Command::Pexpire(key, ttl) => Some(vec![
            b"PEXPIRE".to_vec(),
            key_bytes(key),
            ttl.as_millis().to_string().into_bytes(),
        ]),
        Command::Persist(key) => Some(vec![b"PERSIST".to_vec(), key_bytes(key)]),
        Command::Lpush(key, values) => {
            let mut args = vec![b"LPUSH".to_vec(), key_bytes(key)];
            args.extend(values.iter().cloned());
            Some(args)
        }
        Command::Rpush(key, values) => {
            let mut args = vec![b"RPUSH".to_vec(), key_bytes(key)];
            args.extend(values.iter().cloned());
            Some(args)
        }
        Command::Lpop(key) => Some(vec![b"LPOP".to_vec(), key_bytes(key)]),
        Command::Hset(key, field, value) => Some(vec![
            b"HSET".to_vec(),
            key_bytes(key),
            key_bytes(field),
            value.clone(),
        ]),
        Command::Hdel(key, fields) => {
            let mut args = vec![b"HDEL".to_vec(), key_bytes(key)];
            args.extend(fields.iter().map(|f| key_bytes(f)));
            Some(args)
        }
        Command::Sadd(key, members) => {
            let mut args = vec![b"SADD".to_vec(), key_bytes(key)];
            args.extend(members.iter().cloned());
            Some(args)
        }
        Command::Srem(key, members) => {
            let mut args = vec![b"SREM".to_vec(), key_bytes(key)];
            args.extend(members.iter().cloned());
            Some(args)
        }
        Command::Ping(_)
        | Command::Echo(_)
        | Command::Get(_)
        | Command::Ttl(_)
        | Command::Lrange(..)
        | Command::Hget(..)
        | Command::Hgetall(_)
        | Command::Smembers(_)
        | Command::Sismember(..)
        // Pub/Sub touches no durable state — a subscription is
        // connection-scoped, gone the moment the client disconnects
        // regardless of any AOF, and a published message is never
        // "stored" anywhere, just relayed to whoever happened to be
        // listening at that instant. Real Redis doesn't log these to
        // its AOF either, for the same reason.
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::Publish(..)
        | Command::Unknown(_)
        | Command::WrongArity(_)
        | Command::NotAnInteger => None,
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

    #[test]
    fn write_commands_have_aof_args() {
        assert_eq!(
            aof_args(&Command::Set("k".to_string(), b"v".to_vec())),
            Some(vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Del(vec!["a".to_string(), "b".to_string()])),
            Some(vec![b"DEL".to_vec(), b"a".to_vec(), b"b".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Expire("k".to_string(), Duration::from_secs(60))),
            Some(vec![b"EXPIRE".to_vec(), b"k".to_vec(), b"60".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Pexpire(
                "k".to_string(),
                Duration::from_millis(500)
            )),
            Some(vec![b"PEXPIRE".to_vec(), b"k".to_vec(), b"500".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Persist("k".to_string())),
            Some(vec![b"PERSIST".to_vec(), b"k".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Lpush(
                "l".to_string(),
                vec![b"a".to_vec(), b"b".to_vec()]
            )),
            Some(vec![
                b"LPUSH".to_vec(),
                b"l".to_vec(),
                b"a".to_vec(),
                b"b".to_vec()
            ])
        );
        assert_eq!(
            aof_args(&Command::Rpush("l".to_string(), vec![b"a".to_vec()])),
            Some(vec![b"RPUSH".to_vec(), b"l".to_vec(), b"a".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Lpop("l".to_string())),
            Some(vec![b"LPOP".to_vec(), b"l".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Hset(
                "h".to_string(),
                "f".to_string(),
                b"v".to_vec()
            )),
            Some(vec![
                b"HSET".to_vec(),
                b"h".to_vec(),
                b"f".to_vec(),
                b"v".to_vec()
            ])
        );
        assert_eq!(
            aof_args(&Command::Hdel("h".to_string(), vec!["f".to_string()])),
            Some(vec![b"HDEL".to_vec(), b"h".to_vec(), b"f".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Sadd("s".to_string(), vec![b"a".to_vec()])),
            Some(vec![b"SADD".to_vec(), b"s".to_vec(), b"a".to_vec()])
        );
        assert_eq!(
            aof_args(&Command::Srem("s".to_string(), vec![b"a".to_vec()])),
            Some(vec![b"SREM".to_vec(), b"s".to_vec(), b"a".to_vec()])
        );
    }

    #[test]
    fn read_only_and_error_commands_have_no_aof_args() {
        assert_eq!(aof_args(&Command::Ping(None)), None);
        assert_eq!(aof_args(&Command::Echo(b"x".to_vec())), None);
        assert_eq!(aof_args(&Command::Get("k".to_string())), None);
        assert_eq!(aof_args(&Command::Ttl("k".to_string())), None);
        assert_eq!(aof_args(&Command::Lrange("l".to_string(), 0, -1)), None);
        assert_eq!(
            aof_args(&Command::Hget("h".to_string(), "f".to_string())),
            None
        );
        assert_eq!(aof_args(&Command::Hgetall("h".to_string())), None);
        assert_eq!(aof_args(&Command::Smembers("s".to_string())), None);
        assert_eq!(
            aof_args(&Command::Sismember("s".to_string(), b"m".to_vec())),
            None
        );
        assert_eq!(aof_args(&Command::Unknown(b"FOO".to_vec())), None);
        assert_eq!(aof_args(&Command::WrongArity(b"GET".to_vec())), None);
        assert_eq!(aof_args(&Command::NotAnInteger), None);
    }

    #[test]
    fn subscribe_requires_at_least_one_channel() {
        let (cmd, _) = read_one(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n");
        assert_eq!(cmd, Command::Subscribe(vec!["news".to_string()]));

        let (cmd, _) = read_one(b"*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n$6\r\nsports\r\n");
        assert_eq!(
            cmd,
            Command::Subscribe(vec!["news".to_string(), "sports".to_string()])
        );

        let (cmd, _) = read_one(b"*1\r\n$9\r\nSUBSCRIBE\r\n");
        assert_eq!(cmd, Command::WrongArity(b"SUBSCRIBE".to_vec()));
    }

    #[test]
    fn unsubscribe_with_zero_arguments_is_valid_unlike_other_variadic_commands() {
        let (cmd, _) = read_one(b"*1\r\n$11\r\nUNSUBSCRIBE\r\n");
        assert_eq!(cmd, Command::Unsubscribe(vec![]));

        let (cmd, _) = read_one(b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\nnews\r\n");
        assert_eq!(cmd, Command::Unsubscribe(vec!["news".to_string()]));
    }

    #[test]
    fn publish_requires_exactly_a_channel_and_a_message() {
        let (cmd, _) = read_one(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n");
        assert_eq!(cmd, Command::Publish("news".to_string(), b"hello".to_vec()));

        let (cmd, _) = read_one(b"*2\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n");
        assert_eq!(cmd, Command::WrongArity(b"PUBLISH".to_vec()));
    }

    #[test]
    fn subscribe_unsubscribe_publish_are_never_aof_logged() {
        assert_eq!(
            aof_args(&Command::Subscribe(vec!["news".to_string()])),
            None
        );
        assert_eq!(aof_args(&Command::Unsubscribe(vec![])), None);
        assert_eq!(
            aof_args(&Command::Publish("news".to_string(), b"hi".to_vec())),
            None
        );
    }

    #[test]
    #[should_panic(expected = "dispatch")]
    fn execute_refuses_to_handle_subscribe_directly() {
        // Documents the invariant, rather than just asserting it can't
        // happen: execute() must never be called with a pub/sub
        // command — see the unreachable!() arm in execute() itself.
        execute(&Command::Subscribe(vec!["news".to_string()]), &Store::new());
    }
}
