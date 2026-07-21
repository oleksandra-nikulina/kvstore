//! The shared, thread-safe key-value store — unchanged from stage 6 in
//! every way except this comment, which earns its place: this is the
//! stage that rewrites the *networking* layer onto `tokio`, and the
//! obvious question that raises is "shouldn't this now use
//! `tokio::sync::Mutex` instead of `std::sync::Mutex`?"
//!
//! No — and the reason is worth understanding, not just asserting.
//! `tokio::sync::Mutex` exists for exactly one situation:
//! **`.await` inside the critical section**. A `std::sync::Mutex` guard
//! held across an `.await` is a real problem in async code, because
//! parking a thread on a `std::sync::Mutex` blocks that *entire OS
//! thread* — including every other task tokio has scheduled onto it —
//! for as long as the lock is contended; an `async` mutex instead
//! *yields the task* back to the executor while waiting, so other tasks
//! on that thread keep making progress. But every method below
//! (`get`, `set`, `lpush`, ...) is plain synchronous `HashMap`
//! manipulation with no `.await` anywhere inside the locked region —
//! the guard is acquired and released entirely within one non-async
//! function call, every time. Reaching for `tokio::sync::Mutex` here
//! would add async overhead (it has to interact with the task
//! scheduler) for zero benefit, since there's never anything to yield
//! *to* — the lock is only ever held for a few nanoseconds of pointer
//! chasing. This is exactly the guidance in `tokio::sync::Mutex`'s own
//! documentation: prefer `std::sync::Mutex` when the critical section
//! is short and `.await`-free, even in an otherwise fully async
//! program. See `lib.rs` for where the *networking* code actually needs
//! `tokio`-flavored primitives instead (nothing in `Store` does).

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub type Bytes = Vec<u8>;

/// Hash field names and set/list members are modeled as `Bytes`
/// (binary-safe, like real Redis); hash *field names* are `String` for
/// the same reason top-level keys are — see stage 4's `key_from_bytes`.
pub enum Value {
    Bytes(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<String, Bytes>),
    Set(HashSet<Bytes>),
}

struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|at| now >= at)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct WrongType;

/// The outcome of an `EXPIRE`/`PEXPIRE`. Split out from a plain `bool`
/// because a client-supplied TTL can be large enough that `now + ttl`
/// would overflow `Instant`'s range — that has to be reported, not
/// panicked on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireResult {
    Set,
    Missing,
    Overflow,
}

impl fmt::Display for WrongType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value")
    }
}

/// Looks up `key`, treating an already-expired entry as absent (and
/// removing it on the spot) exactly like every other lazy-expiry check
/// in this store. Shared by every typed operation below so each one
/// doesn't have to repeat the expiry dance.
fn touch<'a>(data: &'a mut HashMap<String, Entry>, key: &str, now: Instant) -> Option<&'a mut Entry> {
    match data.get(key) {
        Some(entry) if entry.is_expired(now) => {
            data.remove(key);
            None
        }
        Some(_) => data.get_mut(key),
        None => None,
    }
}

/// Looks up `key` like [`touch`], but if it's absent (or was just
/// removed for being expired), creates it fresh with `default()` first.
/// Used by the collection commands (`LPUSH`, `HSET`, `SADD`, ...) that
/// auto-vivify a key on first write, same as real Redis.
fn vivify<'a>(
    data: &'a mut HashMap<String, Entry>,
    key: &str,
    now: Instant,
    default: impl FnOnce() -> Value,
) -> &'a mut Entry {
    if touch(data, key, now).is_none() {
        data.insert(
            key.to_string(),
            Entry {
                value: default(),
                expires_at: None,
            },
        );
    }
    data.get_mut(key).unwrap()
}

/// Turns `LRANGE`'s (possibly negative, possibly out-of-bounds) start
/// and stop indices into a valid, inclusive `(start, stop)` pair over a
/// collection of length `len`, or `None` if the resulting range is
/// empty. Negative indices count from the end, same as Python slicing's
/// `-1` meaning "last element."
fn lrange_indices(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i = len as i64;
    let norm_start = |i: i64| if i < 0 { (len_i + i).max(0) } else { i.min(len_i) };
    let norm_stop = |i: i64| if i < 0 { (len_i + i).max(-1) } else { i.min(len_i - 1) };
    let s = norm_start(start);
    let e = norm_stop(stop);
    if s > e || s >= len_i {
        None
    } else {
        Some((s as usize, e as usize))
    }
}

pub struct Store {
    data: Mutex<HashMap<String, Entry>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: Mutex::new(HashMap::new()),
        }
    }

    // ---- plain bytes ----------------------------------------------

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(None);
        };
        match &entry.value {
            Value::Bytes(b) => Ok(Some(b.clone())),
            _ => Err(WrongType),
        }
    }

    /// A full overwrite regardless of the key's previous shape — same
    /// as real Redis, plain `SET` always succeeds and always leaves the
    /// key holding a string, clearing any TTL it had.
    pub fn set(&self, key: String, value: Bytes) {
        self.data.lock().unwrap().insert(
            key,
            Entry {
                value: Value::Bytes(value),
                expires_at: None,
            },
        );
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let mut removed = 0;
        for key in keys {
            if let Some(entry) = data.remove(key)
                && !entry.is_expired(now)
            {
                removed += 1;
            }
        }
        removed
    }

    /// `ttl` comes straight from a client-supplied integer, so it can be
    /// enormous; `Instant + Duration` panics on overflow, so this uses
    /// `checked_add` and reports that case explicitly instead of taking
    /// down the connection thread.
    pub fn expire(&self, key: &str, ttl: Duration) -> ExpireResult {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired(now) => match now.checked_add(ttl) {
                Some(at) => {
                    entry.expires_at = Some(at);
                    ExpireResult::Set
                }
                None => ExpireResult::Overflow,
            },
            Some(_) => {
                data.remove(key);
                ExpireResult::Missing
            }
            None => ExpireResult::Missing,
        }
    }

    pub fn ttl(&self, key: &str) -> Option<Option<Duration>> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        match data.get(key) {
            Some(entry) if entry.is_expired(now) => {
                data.remove(key);
                None
            }
            Some(entry) => Some(entry.expires_at.map(|at| at.saturating_duration_since(now))),
            None => None,
        }
    }

    pub fn persist(&self, key: &str) -> bool {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        match data.get_mut(key) {
            Some(entry) if entry.is_expired(now) => {
                data.remove(key);
                false
            }
            Some(entry) if entry.expires_at.is_some() => {
                entry.expires_at = None;
                true
            }
            Some(_) => false,
            None => false,
        }
    }

    pub fn sweep_expired(&self) {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        data.retain(|_, entry| !entry.is_expired(now));
    }

    // ---- lists ------------------------------------------------------

    /// Pushes each of `values` to the *front*, in the order given — so
    /// `LPUSH key a b c` leaves the list as `c b a`, matching real
    /// Redis (each push lands ahead of the one before it).
    pub fn lpush(&self, key: &str, values: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::List(VecDeque::new()));
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };
        for v in values {
            list.push_front(v.clone());
        }
        Ok(list.len())
    }

    /// Pushes each of `values` to the *back*, in order — `RPUSH key a b
    /// c` leaves the list as `a b c`.
    pub fn rpush(&self, key: &str, values: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::List(VecDeque::new()));
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };
        for v in values {
            list.push_back(v.clone());
        }
        Ok(list.len())
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(Vec::new());
        };
        let Value::List(list) = &entry.value else {
            return Err(WrongType);
        };
        Ok(match lrange_indices(list.len(), start, stop) {
            Some((s, e)) => list.iter().skip(s).take(e - s + 1).cloned().collect(),
            None => Vec::new(),
        })
    }

    /// Pops the front element. If that empties the list, the key is
    /// removed entirely — an empty list isn't a value real Redis (or
    /// this one) leaves lying around.
    pub fn lpop(&self, key: &str) -> Result<Option<Bytes>, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(None);
        };
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };
        let popped = list.pop_front();
        let now_empty = list.is_empty();
        if now_empty {
            data.remove(key);
        }
        Ok(popped)
    }

    // ---- hashes -------------------------------------------------------

    /// Returns whether `field` was newly created (`true`) or already
    /// existed and just got overwritten (`false`) — matches `HSET`'s
    /// reply of "how many new fields," for the single-field form this
    /// stage implements.
    pub fn hset(&self, key: &str, field: String, value: Bytes) -> Result<bool, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::Hash(HashMap::new()));
        let Value::Hash(hash) = &mut entry.value else {
            return Err(WrongType);
        };
        Ok(hash.insert(field, value).is_none())
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(None);
        };
        let Value::Hash(hash) = &entry.value else {
            return Err(WrongType);
        };
        Ok(hash.get(field).cloned())
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(Vec::new());
        };
        let Value::Hash(hash) = &entry.value else {
            return Err(WrongType);
        };
        Ok(hash.iter().map(|(f, v)| (f.clone(), v.clone())).collect())
    }

    /// Removes the given fields, returning how many were actually
    /// present. If that empties the hash, the key is removed entirely.
    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<usize, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(0);
        };
        let Value::Hash(hash) = &mut entry.value else {
            return Err(WrongType);
        };
        let removed = fields.iter().filter(|f| hash.remove(f.as_str()).is_some()).count();
        let now_empty = hash.is_empty();
        if now_empty {
            data.remove(key);
        }
        Ok(removed)
    }

    // ---- sets -----------------------------------------------------

    /// Adds each of `members` that isn't already present, returning how
    /// many were newly added.
    pub fn sadd(&self, key: &str, members: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::Set(HashSet::new()));
        let Value::Set(set) = &mut entry.value else {
            return Err(WrongType);
        };
        let added = members.iter().filter(|m| set.insert((*m).clone())).count();
        Ok(added)
    }

    /// Removes each of `members` that's present, returning how many
    /// were actually removed. If that empties the set, the key is
    /// removed entirely.
    pub fn srem(&self, key: &str, members: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(0);
        };
        let Value::Set(set) = &mut entry.value else {
            return Err(WrongType);
        };
        let removed = members.iter().filter(|m| set.remove(*m)).count();
        let now_empty = set.is_empty();
        if now_empty {
            data.remove(key);
        }
        Ok(removed)
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<Bytes>, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(Vec::new());
        };
        let Value::Set(set) = &entry.value else {
            return Err(WrongType);
        };
        Ok(set.iter().cloned().collect())
    }

    pub fn sismember(&self, key: &str, member: &[u8]) -> Result<bool, WrongType> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(false);
        };
        let Value::Set(set) = &entry.value else {
            return Err(WrongType);
        };
        Ok(set.contains(member))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.data.lock().unwrap().len()
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // ---- bytes + expiry: unchanged behavior from stage 5 ----------

    #[test]
    fn set_then_get_round_trips_the_value() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        assert_eq!(store.get("k"), Ok(Some(b"v".to_vec())));
    }

    #[test]
    fn expire_and_ttl_and_persist_still_work_on_a_string_key() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        assert_eq!(store.expire("k", Duration::from_secs(60)), ExpireResult::Set);
        assert!(store.ttl("k").unwrap().is_some());
        assert!(store.persist("k"));
        assert_eq!(store.ttl("k"), Some(None));
    }

    /// Regression test (found in code review of stage 5, carried
    /// forward here since the same `expire` logic was copied into this
    /// stage): a client-supplied TTL large enough to overflow `Instant`
    /// must not panic the connection thread.
    #[test]
    fn expire_with_a_ttl_that_would_overflow_instant_does_not_panic() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        assert_eq!(
            store.expire("k", Duration::from_secs(u64::MAX)),
            ExpireResult::Overflow
        );

        assert_eq!(store.get("k"), Ok(Some(b"v".to_vec())));
        assert_eq!(store.ttl("k"), Some(None));
    }

    #[test]
    fn concurrent_sets_to_the_same_key_never_produce_a_torn_value() {
        let store = Store::new();
        thread::scope(|scope| {
            for i in 0..32u8 {
                let store = &store;
                scope.spawn(move || store.set("shared".to_string(), vec![i; 64]));
            }
        });
        let value = store.get("shared").unwrap().unwrap();
        assert!(value.iter().all(|&b| b == value[0]));
    }

    // ---- lists ------------------------------------------------------

    #[test]
    fn lpush_prepends_each_value_reversing_argument_order() {
        let store = Store::new();
        assert_eq!(store.lpush("l", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]), Ok(3));
        assert_eq!(
            store.lrange("l", 0, -1),
            Ok(vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()])
        );
    }

    #[test]
    fn rpush_appends_each_value_preserving_argument_order() {
        let store = Store::new();
        assert_eq!(store.rpush("l", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]), Ok(3));
        assert_eq!(
            store.lrange("l", 0, -1),
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
    }

    #[test]
    fn lrange_on_a_missing_key_is_an_empty_list_not_an_error() {
        let store = Store::new();
        assert_eq!(store.lrange("missing", 0, -1), Ok(Vec::new()));
    }

    #[test]
    fn lrange_handles_negative_and_out_of_bounds_indices() {
        let store = Store::new();
        store.rpush("l", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).unwrap();

        assert_eq!(store.lrange("l", -2, -1), Ok(vec![b"b".to_vec(), b"c".to_vec()]));
        assert_eq!(
            store.lrange("l", 0, 100),
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(store.lrange("l", 5, 10), Ok(Vec::new()));
        assert_eq!(store.lrange("l", 2, 1), Ok(Vec::new()));
    }

    #[test]
    fn lpop_removes_the_front_element_and_deletes_an_emptied_list() {
        let store = Store::new();
        store.rpush("l", &[b"a".to_vec(), b"b".to_vec()]).unwrap();

        assert_eq!(store.lpop("l"), Ok(Some(b"a".to_vec())));
        assert_eq!(store.lpop("l"), Ok(Some(b"b".to_vec())));
        assert_eq!(store.lpop("l"), Ok(None));
        assert_eq!(store.len(), 0, "an emptied list should not linger as a key");
    }

    #[test]
    fn list_ops_against_a_string_key_are_wrongtype() {
        let store = Store::new();
        store.set("s".to_string(), b"v".to_vec());
        assert_eq!(store.lpush("s", &[b"x".to_vec()]), Err(WrongType));
        assert_eq!(store.rpush("s", &[b"x".to_vec()]), Err(WrongType));
        assert_eq!(store.lrange("s", 0, -1), Err(WrongType));
        assert_eq!(store.lpop("s"), Err(WrongType));
    }

    // ---- hashes ---------------------------------------------------

    #[test]
    fn hset_reports_whether_the_field_was_new() {
        let store = Store::new();
        assert_eq!(store.hset("h", "f".to_string(), b"1".to_vec()), Ok(true));
        assert_eq!(store.hset("h", "f".to_string(), b"2".to_vec()), Ok(false));
        assert_eq!(store.hget("h", "f"), Ok(Some(b"2".to_vec())));
    }

    #[test]
    fn hgetall_on_a_missing_key_is_empty() {
        let store = Store::new();
        assert_eq!(store.hgetall("missing"), Ok(Vec::new()));
    }

    #[test]
    fn hgetall_returns_every_field_value_pair() {
        let store = Store::new();
        store.hset("h", "a".to_string(), b"1".to_vec()).unwrap();
        store.hset("h", "b".to_string(), b"2".to_vec()).unwrap();

        let mut pairs = store.hgetall("h").unwrap();
        pairs.sort();
        assert_eq!(pairs, vec![("a".to_string(), b"1".to_vec()), ("b".to_string(), b"2".to_vec())]);
    }

    #[test]
    fn hdel_removes_fields_and_deletes_an_emptied_hash() {
        let store = Store::new();
        store.hset("h", "a".to_string(), b"1".to_vec()).unwrap();
        store.hset("h", "b".to_string(), b"2".to_vec()).unwrap();

        assert_eq!(store.hdel("h", &["a".to_string(), "missing".to_string()]), Ok(1));
        assert_eq!(store.hdel("h", &["b".to_string()]), Ok(1));
        assert_eq!(store.len(), 0, "an emptied hash should not linger as a key");
    }

    #[test]
    fn hash_ops_against_a_string_key_are_wrongtype() {
        let store = Store::new();
        store.set("s".to_string(), b"v".to_vec());
        assert_eq!(store.hset("s", "f".to_string(), b"v".to_vec()), Err(WrongType));
        assert_eq!(store.hget("s", "f"), Err(WrongType));
        assert_eq!(store.hgetall("s"), Err(WrongType));
        assert_eq!(store.hdel("s", &["f".to_string()]), Err(WrongType));
    }

    // ---- sets -------------------------------------------------------

    #[test]
    fn sadd_reports_only_newly_added_members() {
        let store = Store::new();
        assert_eq!(store.sadd("s", &[b"a".to_vec(), b"b".to_vec()]), Ok(2));
        assert_eq!(store.sadd("s", &[b"b".to_vec(), b"c".to_vec()]), Ok(1));

        let mut members = store.smembers("s").unwrap();
        members.sort();
        assert_eq!(members, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn sismember_reflects_membership() {
        let store = Store::new();
        store.sadd("s", &[b"a".to_vec()]).unwrap();
        assert_eq!(store.sismember("s", b"a"), Ok(true));
        assert_eq!(store.sismember("s", b"b"), Ok(false));
        assert_eq!(store.sismember("missing", b"a"), Ok(false));
    }

    #[test]
    fn srem_removes_members_and_deletes_an_emptied_set() {
        let store = Store::new();
        store.sadd("s", &[b"a".to_vec(), b"b".to_vec()]).unwrap();

        assert_eq!(store.srem("s", &[b"a".to_vec(), b"missing".to_vec()]), Ok(1));
        assert_eq!(store.srem("s", &[b"b".to_vec()]), Ok(1));
        assert_eq!(store.len(), 0, "an emptied set should not linger as a key");
    }

    #[test]
    fn set_ops_against_a_string_key_are_wrongtype() {
        let store = Store::new();
        store.set("str".to_string(), b"v".to_vec());
        assert_eq!(store.sadd("str", &[b"x".to_vec()]), Err(WrongType));
        assert_eq!(store.srem("str", &[b"x".to_vec()]), Err(WrongType));
        assert_eq!(store.smembers("str"), Err(WrongType));
        assert_eq!(store.sismember("str", b"x"), Err(WrongType));
    }

    #[test]
    fn get_on_a_list_key_is_wrongtype() {
        let store = Store::new();
        store.rpush("l", &[b"a".to_vec()]).unwrap();
        assert_eq!(store.get("l"), Err(WrongType));
    }
}
