//! The shared, thread-safe key-value store — now holding more than one
//! shape of value per key. A single `HashMap<String, Entry>` still backs
//! everything; what's new is that `Entry`'s value is a [`Value`] enum
//! (plain bytes, a list, a hash, or a set) instead of always being
//! bytes, and every collection operation has to check the stored
//! [`Value`] variant matches what the command expects — mismatched
//! (`LPUSH` on a key holding a plain string, say) is a [`WrongType`]
//! error, exactly like real Redis's `WRONGTYPE`.
//!
//! A single `RwLock`, not a `Mutex`: every genuinely read-only method
//! (`get`, `ttl`, `lrange`, `hget`, `hgetall`, `smembers`, `sismember`)
//! takes a shared read lock via [`peek`] and can run concurrently with
//! other reads; every method that mutates (`set`, `del`,
//! `lpush`/`rpush`/`lpop`, `hset`/`hdel`, `sadd`/`srem`, `expire`,
//! `persist`, `sweep_expired`) still needs exclusive access via
//! [`touch`]/[`vivify`], same as before. See `DESIGN_TRADEOFFS_NOTES.md`
//! at the project root for the full reasoning on why this is a real but
//! bounded improvement, not a free lunch: `RwLock` isn't "cheaper" than
//! `Mutex` per call, `std::sync::RwLock` makes no fairness guarantee
//! (a writer could in principle be starved by steady read traffic), and
//! it does nothing for the deeper one-lock-over-the-whole-keyspace
//! ceiling — that needs sharding, out of scope here.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::RwLock;
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
        write!(
            f,
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
    }
}

/// Read-only lookup: treats an expired entry as absent **without
/// removing it**. This is what lets every genuinely read-only method
/// below take a shared `RwLock::read()` guard (`&HashMap`, not `&mut`)
/// instead of an exclusive write guard — a real non-mutating read path,
/// not just a relabeled write path. The trade-off: an expired key
/// nobody ever writes through again now only gets physically removed by
/// the active sweep (or a `touch`/`vivify`-based write-path operation
/// that happens to land on it), not by the read that first observes it
/// as gone. Observably identical to a client either way.
fn peek<'a>(data: &'a HashMap<String, Entry>, key: &str, now: Instant) -> Option<&'a Entry> {
    match data.get(key) {
        Some(entry) if !entry.is_expired(now) => Some(entry),
        _ => None,
    }
}

/// Looks up `key` on the *write* path, treating an already-expired entry
/// as absent and removing it on the spot — every caller here already
/// holds an exclusive write guard for its own reasons (it's about to
/// mutate regardless), so doing the cleanup here costs nothing extra,
/// unlike on the read path (see [`peek`]).
fn touch<'a>(
    data: &'a mut HashMap<String, Entry>,
    key: &str,
    now: Instant,
) -> Option<&'a mut Entry> {
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
    let norm_start = |i: i64| {
        if i < 0 {
            (len_i + i).max(0)
        } else {
            i.min(len_i)
        }
    };
    let norm_stop = |i: i64| {
        if i < 0 {
            (len_i + i).max(-1)
        } else {
            i.min(len_i - 1)
        }
    };
    let s = norm_start(start);
    let e = norm_stop(stop);
    if s > e || s >= len_i {
        None
    } else {
        Some((s as usize, e as usize))
    }
}

pub struct Store {
    data: RwLock<HashMap<String, Entry>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: RwLock::new(HashMap::new()),
        }
    }

    // ---- plain bytes ----------------------------------------------

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
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
        self.data.write().unwrap().insert(
            key,
            Entry {
                value: Value::Bytes(value),
                expires_at: None,
            },
        );
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut data = self.data.write().unwrap();
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
        let mut data = self.data.write().unwrap();
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

    /// Pure read path — see [`peek`].
    pub fn ttl(&self, key: &str) -> Option<Option<Duration>> {
        let data = self.data.read().unwrap();
        let now = Instant::now();
        peek(&data, key, now)
            .map(|entry| entry.expires_at.map(|at| at.saturating_duration_since(now)))
    }

    pub fn persist(&self, key: &str) -> bool {
        let mut data = self.data.write().unwrap();
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

    /// The primary cleanup mechanism for an expired key that's never
    /// read or written through again — see [`peek`]'s doc comment for
    /// why the read path no longer does this itself.
    pub fn sweep_expired(&self) {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        data.retain(|_, entry| !entry.is_expired(now));
    }

    // ---- lists ------------------------------------------------------

    /// Pushes each of `values` to the *front*, in the order given — so
    /// `LPUSH key a b c` leaves the list as `c b a`, matching real
    /// Redis (each push lands ahead of the one before it).
    pub fn lpush(&self, key: &str, values: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.write().unwrap();
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
        let mut data = self.data.write().unwrap();
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

    /// Pure read path — see [`peek`].
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
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
        let mut data = self.data.write().unwrap();
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
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::Hash(HashMap::new()));
        let Value::Hash(hash) = &mut entry.value else {
            return Err(WrongType);
        };
        Ok(hash.insert(field, value).is_none())
    }

    /// Pure read path — see [`peek`].
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
            return Ok(None);
        };
        let Value::Hash(hash) = &entry.value else {
            return Err(WrongType);
        };
        Ok(hash.get(field).cloned())
    }

    /// Pure read path — see [`peek`].
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
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
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(0);
        };
        let Value::Hash(hash) = &mut entry.value else {
            return Err(WrongType);
        };
        let removed = fields
            .iter()
            .filter(|f| hash.remove(f.as_str()).is_some())
            .count();
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
        let mut data = self.data.write().unwrap();
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
        let mut data = self.data.write().unwrap();
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

    /// Pure read path — see [`peek`].
    pub fn smembers(&self, key: &str) -> Result<Vec<Bytes>, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
            return Ok(Vec::new());
        };
        let Value::Set(set) = &entry.value else {
            return Err(WrongType);
        };
        Ok(set.iter().cloned().collect())
    }

    /// Pure read path — see [`peek`].
    pub fn sismember(&self, key: &str, member: &[u8]) -> Result<bool, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
            return Ok(false);
        };
        let Value::Set(set) = &entry.value else {
            return Err(WrongType);
        };
        Ok(set.contains(member))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.data.read().unwrap().len()
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
        assert_eq!(
            store.expire("k", Duration::from_secs(60)),
            ExpireResult::Set
        );
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

    /// New in the RwLock migration: many threads concurrently reading
    /// the same key (a plain value here, but `peek` is the same code
    /// path every read-only method uses) must all see a consistent
    /// value via the shared read lock.
    #[test]
    fn concurrent_reads_of_the_same_key_are_all_consistent() {
        let store = Store::new();
        store.set("shared".to_string(), vec![9u8; 64]);

        thread::scope(|scope| {
            for _ in 0..32 {
                let store = &store;
                scope.spawn(move || {
                    assert_eq!(store.get("shared"), Ok(Some(vec![9u8; 64])));
                });
            }
        });
    }

    /// An expired key is invisible to reads immediately, but — unlike
    /// before the RwLock migration — reading it doesn't physically
    /// remove it; only a write-path touch or the sweep does.
    #[test]
    fn an_expired_key_lingers_physically_until_a_write_path_operation_or_sweep_touches_it() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_millis(1));
        thread::sleep(Duration::from_millis(20));

        assert_eq!(store.get("k"), Ok(None));
        assert_eq!(store.ttl("k"), None);
        assert_eq!(
            store.len(),
            1,
            "the expired entry should still physically occupy its map slot"
        );

        store.sweep_expired();
        assert_eq!(store.len(), 0);
    }

    // ---- lists ------------------------------------------------------

    #[test]
    fn lpush_prepends_each_value_reversing_argument_order() {
        let store = Store::new();
        assert_eq!(
            store.lpush("l", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
            Ok(3)
        );
        assert_eq!(
            store.lrange("l", 0, -1),
            Ok(vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()])
        );
    }

    #[test]
    fn rpush_appends_each_value_preserving_argument_order() {
        let store = Store::new();
        assert_eq!(
            store.rpush("l", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
            Ok(3)
        );
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
        store
            .rpush("l", &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .unwrap();

        assert_eq!(
            store.lrange("l", -2, -1),
            Ok(vec![b"b".to_vec(), b"c".to_vec()])
        );
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
        assert_eq!(
            pairs,
            vec![
                ("a".to_string(), b"1".to_vec()),
                ("b".to_string(), b"2".to_vec())
            ]
        );
    }

    #[test]
    fn hdel_removes_fields_and_deletes_an_emptied_hash() {
        let store = Store::new();
        store.hset("h", "a".to_string(), b"1".to_vec()).unwrap();
        store.hset("h", "b".to_string(), b"2".to_vec()).unwrap();

        assert_eq!(
            store.hdel("h", &["a".to_string(), "missing".to_string()]),
            Ok(1)
        );
        assert_eq!(store.hdel("h", &["b".to_string()]), Ok(1));
        assert_eq!(store.len(), 0, "an emptied hash should not linger as a key");
    }

    #[test]
    fn hash_ops_against_a_string_key_are_wrongtype() {
        let store = Store::new();
        store.set("s".to_string(), b"v".to_vec());
        assert_eq!(
            store.hset("s", "f".to_string(), b"v".to_vec()),
            Err(WrongType)
        );
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

        assert_eq!(
            store.srem("s", &[b"a".to_vec(), b"missing".to_vec()]),
            Ok(1)
        );
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
