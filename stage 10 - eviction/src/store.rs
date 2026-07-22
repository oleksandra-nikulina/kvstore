//! The shared, thread-safe key-value store — now with an optional
//! approximate memory cap and an eviction policy (LRU or LFU, see
//! `eviction.rs`) that kicks in once it's exceeded.
//!
//! `StoreData` bundles the actual `HashMap` with a running
//! `approx_bytes` total so the two always update together under one
//! lock — `approx_bytes` is a *logical* size estimate (sum of key and
//! value byte lengths), not real process memory; it ignores `HashMap`
//! bucket overhead, allocator overhead, and Rust struct sizes, same
//! spirit as real Redis's own `maxmemory` accounting being an
//! approximation rather than an exact measurement.
//!
//! Recency/frequency tracking for eviction lives in a *separate* lock
//! ([`crate::eviction::Eviction`]) from this store's own `RwLock` — see
//! that module's doc comment for why, and for the one narrow
//! consequence that decoupling accepts (handled defensively in
//! [`Store::maybe_evict`] below).

use crate::eviction::{Eviction, Policy};
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

fn value_size(value: &Value) -> usize {
    match value {
        Value::Bytes(b) => b.len(),
        Value::List(l) => l.iter().map(Vec::len).sum(),
        Value::Hash(h) => h.iter().map(|(f, v)| f.len() + v.len()).sum(),
        Value::Set(s) => s.iter().map(Vec::len).sum(),
    }
}

fn entry_size(key: &str, entry: &Entry) -> usize {
    key.len() + value_size(&entry.value)
}

/// The map plus a running total of [`entry_size`] across every entry in
/// it, kept in sync by every method below rather than recomputed from
/// scratch (which would be O(n) per write).
struct StoreData {
    map: HashMap<String, Entry>,
    approx_bytes: usize,
}

impl StoreData {
    fn new() -> Self {
        StoreData {
            map: HashMap::new(),
            approx_bytes: 0,
        }
    }

    fn size_of(&self, key: &str) -> usize {
        self.map.get(key).map(|e| entry_size(key, e)).unwrap_or(0)
    }

    /// Adjusts `approx_bytes` from `old_size` to `new_size` for one
    /// entry. Takes both rather than just a signed delta specifically
    /// to avoid ever computing a delta in a way that could underflow a
    /// `usize` at the call site — this figures out the direction itself.
    fn resize(&mut self, old_size: usize, new_size: usize) {
        if new_size >= old_size {
            self.approx_bytes += new_size - old_size;
        } else {
            self.approx_bytes -= old_size - new_size;
        }
    }
}

/// Read-only lookup: treats an expired entry as absent **without
/// removing it** — see stage 5's original version of this function for
/// the full reasoning (unchanged by this stage). Never touches
/// `approx_bytes`, since it never mutates.
fn peek<'a>(data: &'a StoreData, key: &str, now: Instant) -> Option<&'a Entry> {
    match data.map.get(key) {
        Some(entry) if !entry.is_expired(now) => Some(entry),
        _ => None,
    }
}

/// Looks up `key` on the write path, removing it if expired. Unlike
/// stage 6-9's version, this one takes `&mut StoreData` (not just
/// `&mut HashMap`) specifically so it can account for its own removal —
/// every call site gets that accounting for free instead of having to
/// remember to do it themselves.
fn touch<'a>(data: &'a mut StoreData, key: &str, now: Instant) -> Option<&'a mut Entry> {
    match data.map.get(key) {
        Some(entry) if entry.is_expired(now) => {
            let size = entry_size(key, entry);
            data.map.remove(key);
            data.approx_bytes -= size;
            None
        }
        Some(_) => data.map.get_mut(key),
        None => None,
    }
}

/// Looks up `key` like [`touch`], but creates it fresh with `default()`
/// first if absent (or just removed for being expired). A freshly
/// vivified entry always starts as an empty collection, so its size
/// contribution is exactly the key's own length — accounted for here;
/// the caller is about to add real content on top and accounts for that
/// itself.
fn vivify<'a>(
    data: &'a mut StoreData,
    key: &str,
    now: Instant,
    default: impl FnOnce() -> Value,
) -> &'a mut Entry {
    if touch(data, key, now).is_none() {
        data.map.insert(
            key.to_string(),
            Entry {
                value: default(),
                expires_at: None,
            },
        );
        let size = entry_size(key, data.map.get(key).unwrap());
        data.approx_bytes += size;
    }
    data.map.get_mut(key).unwrap()
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

/// Configured only when the server was started with `--maxmemory`; its
/// absence (`Store::new()`, no cap) is what every earlier stage's
/// behavior — and every test carried forward from them — still gets.
struct MemoryLimit {
    maxmemory: usize,
    eviction: Eviction,
}

pub struct Store {
    data: RwLock<StoreData>,
    limit: Option<MemoryLimit>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: RwLock::new(StoreData::new()),
            limit: None,
        }
    }

    /// A store that evicts keys under `policy` once its approximate
    /// size exceeds `maxmemory` bytes.
    pub fn with_eviction(maxmemory: usize, policy: Policy) -> Self {
        Store {
            data: RwLock::new(StoreData::new()),
            limit: Some(MemoryLimit {
                maxmemory,
                eviction: Eviction::new(policy),
            }),
        }
    }

    /// Records `key` as accessed for eviction purposes — a no-op if
    /// this store has no configured limit. Called for every command
    /// that actually reads or writes a key's *value*; deliberately not
    /// called for `EXPIRE`/`PERSIST`/`TTL`/`DEL`, which this project
    /// treats as metadata-only operations rather than value accesses.
    fn record_access(&self, key: &str) {
        if let Some(limit) = &self.limit {
            limit.eviction.record_access(key);
        }
    }

    /// Evicts keys (per the configured policy) until the store is back
    /// under `maxmemory`, or until there's nothing *else* left to evict
    /// — whichever comes first.
    ///
    /// `protect`, when given, is the key whose own write just triggered
    /// this call (e.g. `SET`'s key), and is never itself chosen as the
    /// victim — see [`crate::eviction::Eviction::evict_except`]. Without
    /// this, a `SET` of a value that alone exceeds `maxmemory` on a
    /// freshly-touched key could evict *that same key* — under LRU
    /// because it's briefly the only tracked entry, or under LFU because
    /// a brand-new key starts at the *lowest* frequency and is often the
    /// very first candidate offered — silently discarding the write the
    /// client just asked for. Real Redis doesn't do that either: once
    /// there's nothing left to evict *other than* the key just written,
    /// it gives up and stays over the cap rather than deleting the data
    /// it was just asked to save.
    ///
    /// Also handles the eviction-tracker/store race documented on
    /// [`crate::eviction::Eviction::record_access`] defensively: if the
    /// key the tracker hands back is already gone from the store (a
    /// stale/"phantom" entry left over from that race), this just loops
    /// and asks for another victim instead of assuming eviction always
    /// frees memory. Either way — a real eviction or a phantom — the
    /// tracker shrinks by at least one entry per iteration, so this is
    /// guaranteed to terminate.
    fn maybe_evict(&self, protect: Option<&str>) {
        let Some(limit) = &self.limit else { return };
        loop {
            let over_cap = self.data.read().unwrap().approx_bytes > limit.maxmemory;
            if !over_cap {
                return;
            }
            let Some(victim) = limit.eviction.evict_except(protect) else {
                return;
            };
            let mut data = self.data.write().unwrap();
            if let Some(entry) = data.map.remove(&victim) {
                let size = entry_size(&victim, &entry);
                data.approx_bytes -= size;
            }
        }
    }

    // ---- plain bytes ----------------------------------------------

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, WrongType> {
        let data = self.data.read().unwrap();
        let Some(entry) = peek(&data, key, Instant::now()) else {
            return Ok(None);
        };
        let Value::Bytes(b) = &entry.value else {
            // Found something, but not the right shape - matches every
            // other typed read (lrange/hget/hgetall/smembers/sismember):
            // a WRONGTYPE doesn't count as a successful access. Found
            // by review: this used to fall through to record_access
            // unconditionally, inconsistent with all five siblings.
            return Err(WrongType);
        };
        let result = Ok(Some(b.clone()));
        drop(data);
        self.record_access(key);
        result
    }

    /// A full overwrite regardless of the key's previous shape — same
    /// as real Redis, plain `SET` always succeeds and always leaves the
    /// key holding a string, clearing any TTL it had.
    pub fn set(&self, key: String, value: Bytes) {
        let mut data = self.data.write().unwrap();
        let old_size = data.size_of(&key);
        data.map.insert(
            key.clone(),
            Entry {
                value: Value::Bytes(value),
                expires_at: None,
            },
        );
        let new_size = data.size_of(&key);
        data.resize(old_size, new_size);
        drop(data);
        self.record_access(&key);
        self.maybe_evict(Some(&key));
    }

    /// Removes every key in `keys` that's present *and not already
    /// expired*, returning how many of those actually existed — an
    /// already-expired key is treated as absent even if it hasn't been
    /// swept yet, same as `GET` would treat it.
    pub fn del(&self, keys: &[String]) -> usize {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let mut removed = 0;
        for key in keys {
            if let Some(entry) = data.map.remove(key) {
                data.approx_bytes -= entry_size(key, &entry);
                if !entry.is_expired(now) {
                    removed += 1;
                }
            }
        }
        drop(data);
        if let Some(limit) = &self.limit {
            for key in keys {
                limit.eviction.forget(key);
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
        let (result, expired_away) = match data.map.get_mut(key) {
            Some(entry) if !entry.is_expired(now) => match now.checked_add(ttl) {
                Some(at) => {
                    entry.expires_at = Some(at);
                    (ExpireResult::Set, false)
                }
                None => (ExpireResult::Overflow, false),
            },
            Some(entry) => {
                let size = entry_size(key, entry);
                data.map.remove(key);
                data.approx_bytes -= size;
                (ExpireResult::Missing, true)
            }
            None => (ExpireResult::Missing, false),
        };
        drop(data);
        if expired_away && let Some(limit) = &self.limit {
            limit.eviction.forget(key);
        }
        result
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
        let (result, expired_away) = match data.map.get_mut(key) {
            Some(entry) if entry.is_expired(now) => {
                let size = entry_size(key, entry);
                data.map.remove(key);
                data.approx_bytes -= size;
                (false, true)
            }
            Some(entry) if entry.expires_at.is_some() => {
                entry.expires_at = None;
                (true, false)
            }
            Some(_) => (false, false),
            None => (false, false),
        };
        drop(data);
        if expired_away && let Some(limit) = &self.limit {
            limit.eviction.forget(key);
        }
        result
    }

    /// The primary cleanup mechanism for an expired key that's never
    /// read or written through again — see [`peek`]'s doc comment.
    pub fn sweep_expired(&self) {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let expired_keys: Vec<String> = data
            .map
            .iter()
            .filter(|(_, entry)| entry.is_expired(now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired_keys {
            if let Some(entry) = data.map.remove(key) {
                data.approx_bytes -= entry_size(key, &entry);
            }
        }
        drop(data);
        if let Some(limit) = &self.limit {
            for key in &expired_keys {
                limit.eviction.forget(key);
            }
        }
    }

    // ---- lists ------------------------------------------------------

    /// Pushes each of `values` to the *front*, in the order given — so
    /// `LPUSH key a b c` leaves the list as `c b a`, matching real
    /// Redis (each push lands ahead of the one before it).
    pub fn lpush(&self, key: &str, values: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::List(VecDeque::new()));
        let old_size = entry_size(key, entry);
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };
        for v in values {
            list.push_front(v.clone());
        }
        let len = list.len();
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        drop(data);
        self.record_access(key);
        self.maybe_evict(Some(key));
        Ok(len)
    }

    /// Pushes each of `values` to the *back*, in order — `RPUSH key a b
    /// c` leaves the list as `a b c`.
    pub fn rpush(&self, key: &str, values: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::List(VecDeque::new()));
        let old_size = entry_size(key, entry);
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };
        for v in values {
            list.push_back(v.clone());
        }
        let len = list.len();
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        drop(data);
        self.record_access(key);
        self.maybe_evict(Some(key));
        Ok(len)
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
        let result = match lrange_indices(list.len(), start, stop) {
            Some((s, e)) => list.iter().skip(s).take(e - s + 1).cloned().collect(),
            None => Vec::new(),
        };
        drop(data);
        self.record_access(key);
        Ok(result)
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
        let old_size = entry_size(key, entry);
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };
        let popped = list.pop_front();
        let now_empty = list.is_empty();
        if now_empty {
            data.map.remove(key);
        }
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        let accessed = popped.is_some();
        drop(data);
        if accessed {
            self.record_access(key);
        }
        self.maybe_evict(None);
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
        let old_size = entry_size(key, entry);
        let Value::Hash(hash) = &mut entry.value else {
            return Err(WrongType);
        };
        let is_new = hash.insert(field, value).is_none();
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        drop(data);
        self.record_access(key);
        self.maybe_evict(Some(key));
        Ok(is_new)
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
        let result = hash.get(field).cloned();
        drop(data);
        self.record_access(key);
        Ok(result)
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
        let result = hash.iter().map(|(f, v)| (f.clone(), v.clone())).collect();
        drop(data);
        self.record_access(key);
        Ok(result)
    }

    /// Removes the given fields, returning how many were actually
    /// present. If that empties the hash, the key is removed entirely.
    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<usize, WrongType> {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let Some(entry) = touch(&mut data, key, now) else {
            return Ok(0);
        };
        let old_size = entry_size(key, entry);
        let Value::Hash(hash) = &mut entry.value else {
            return Err(WrongType);
        };
        let removed = fields
            .iter()
            .filter(|f| hash.remove(f.as_str()).is_some())
            .count();
        let now_empty = hash.is_empty();
        if now_empty {
            data.map.remove(key);
        }
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        drop(data);
        if removed > 0 {
            self.record_access(key);
        }
        self.maybe_evict(None);
        Ok(removed)
    }

    // ---- sets -----------------------------------------------------

    /// Adds each of `members` that isn't already present, returning how
    /// many were newly added.
    pub fn sadd(&self, key: &str, members: &[Bytes]) -> Result<usize, WrongType> {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        let entry = vivify(&mut data, key, now, || Value::Set(HashSet::new()));
        let old_size = entry_size(key, entry);
        let Value::Set(set) = &mut entry.value else {
            return Err(WrongType);
        };
        let added = members.iter().filter(|m| set.insert((*m).clone())).count();
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        drop(data);
        self.record_access(key);
        self.maybe_evict(Some(key));
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
        let old_size = entry_size(key, entry);
        let Value::Set(set) = &mut entry.value else {
            return Err(WrongType);
        };
        let removed = members.iter().filter(|m| set.remove(*m)).count();
        let now_empty = set.is_empty();
        if now_empty {
            data.map.remove(key);
        }
        let new_size = data.size_of(key);
        data.resize(old_size, new_size);
        drop(data);
        if removed > 0 {
            self.record_access(key);
        }
        self.maybe_evict(None);
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
        let result = set.iter().cloned().collect();
        drop(data);
        self.record_access(key);
        Ok(result)
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
        let result = set.contains(member);
        drop(data);
        self.record_access(key);
        Ok(result)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.data.read().unwrap().map.len()
    }

    #[cfg(test)]
    fn approx_bytes(&self) -> usize {
        self.data.read().unwrap().approx_bytes
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

    // ---- bytes + expiry: unchanged behavior from stage 5-9 --------

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

    #[test]
    fn expire_with_a_ttl_that_would_overflow_instant_does_not_panic() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        assert_eq!(
            store.expire("k", Duration::from_secs(u64::MAX)),
            ExpireResult::Overflow
        );
        assert_eq!(store.get("k"), Ok(Some(b"v".to_vec())));
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

    #[test]
    fn an_expired_key_lingers_physically_until_a_write_path_operation_or_sweep_touches_it() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_millis(1));
        thread::sleep(Duration::from_millis(20));
        assert_eq!(store.get("k"), Ok(None));
        assert_eq!(store.len(), 1);
        store.sweep_expired();
        assert_eq!(store.len(), 0);
    }

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
    fn lpop_removes_the_front_element_and_deletes_an_emptied_list() {
        let store = Store::new();
        store.rpush("l", &[b"a".to_vec(), b"b".to_vec()]).unwrap();
        assert_eq!(store.lpop("l"), Ok(Some(b"a".to_vec())));
        assert_eq!(store.lpop("l"), Ok(Some(b"b".to_vec())));
        assert_eq!(store.lpop("l"), Ok(None));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn hset_hget_hdel_round_trip_and_delete_an_emptied_hash() {
        let store = Store::new();
        assert_eq!(store.hset("h", "f".to_string(), b"1".to_vec()), Ok(true));
        assert_eq!(store.hget("h", "f"), Ok(Some(b"1".to_vec())));
        assert_eq!(store.hdel("h", &["f".to_string()]), Ok(1));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn sadd_srem_round_trip_and_delete_an_emptied_set() {
        let store = Store::new();
        assert_eq!(store.sadd("s", &[b"a".to_vec()]), Ok(1));
        assert_eq!(store.sismember("s", b"a"), Ok(true));
        assert_eq!(store.srem("s", &[b"a".to_vec()]), Ok(1));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn wrongtype_errors_still_fire_for_every_typed_op() {
        let store = Store::new();
        store.set("s".to_string(), b"v".to_vec());
        assert_eq!(store.lpush("s", &[b"x".to_vec()]), Err(WrongType));
        assert_eq!(
            store.hset("s", "f".to_string(), b"v".to_vec()),
            Err(WrongType)
        );
        assert_eq!(store.sadd("s", &[b"x".to_vec()]), Err(WrongType));
        assert_eq!(store.get("s"), Ok(Some(b"v".to_vec())));
    }

    /// Regression test for a bug found in review: `get()` used to call
    /// `record_access` even when the key turned out to be the wrong
    /// type, inconsistent with every sibling read method
    /// (`lrange`/`hget`/`hgetall`/`smembers`/`sismember`), all of which
    /// only count a genuinely successful read as an access. A failed
    /// `GET` on a list key shouldn't be able to protect that key from
    /// eviction.
    #[test]
    fn a_wrongtype_get_does_not_count_as_an_access_for_eviction() {
        // "list-key"(8)+"x"(1)=9 bytes; "other"(5)+5=10 bytes; total 19,
        // comfortably under a 35-byte cap so setup itself never evicts.
        let store = Store::with_eviction(35, Policy::Lru);
        store.rpush("list-key", &[b"x".to_vec()]).unwrap();
        store.set("other".to_string(), vec![0u8; 5]);

        // Repeatedly (and unsuccessfully) GET the list key - none of
        // these should count as an access.
        for _ in 0..10 {
            assert_eq!(store.get("list-key"), Err(WrongType));
        }

        // "list-key" was touched once, by rpush, before "other" was
        // set - so it's still the least recently used entry despite the
        // 10 failed GETs. "new-key"(7)+10=17; 19+17=36, over the
        // 35-byte cap by 1 - evicting list-key's 9 bytes alone
        // (36-9=27) is enough, so exactly it (not "other") is evicted.
        store.set("new-key".to_string(), vec![0u8; 10]);

        assert_eq!(
            store.lrange("list-key", 0, -1),
            Ok(Vec::new()),
            "list-key should have been evicted"
        );
        assert_eq!(store.get("other"), Ok(Some(vec![0u8; 5])));
    }

    // ---- byte accounting -------------------------------------------

    #[test]
    fn approx_bytes_tracks_inserts_updates_and_removals() {
        let store = Store::new();
        assert_eq!(store.approx_bytes(), 0);

        store.set("k".to_string(), b"hello".to_vec()); // "k" (1) + "hello" (5)
        assert_eq!(store.approx_bytes(), 6);

        store.set("k".to_string(), b"hi".to_vec()); // overwrite: "k" (1) + "hi" (2)
        assert_eq!(store.approx_bytes(), 3);

        store.del(&["k".to_string()]);
        assert_eq!(store.approx_bytes(), 0);
    }

    #[test]
    fn approx_bytes_tracks_collection_growth_and_shrinkage() {
        let store = Store::new();
        store.rpush("l", &[b"aa".to_vec(), b"bb".to_vec()]).unwrap(); // "l"(1) + 2 + 2
        assert_eq!(store.approx_bytes(), 5);
        store.lpop("l").unwrap();
        assert_eq!(store.approx_bytes(), 3); // "l"(1) + "bb"(2)
        store.lpop("l").unwrap(); // list now empty -> key removed entirely
        assert_eq!(store.approx_bytes(), 0);
    }

    #[test]
    fn expiring_a_key_away_removes_its_bytes_too() {
        let store = Store::new();
        store.set("k".to_string(), b"value".to_vec());
        store.expire("k", Duration::from_millis(1));
        thread::sleep(Duration::from_millis(20));
        // Lazily expired on the write path here (persist touches it).
        store.persist("k");
        assert_eq!(store.approx_bytes(), 0);
    }

    // ---- eviction ---------------------------------------------------

    #[test]
    fn a_store_without_a_configured_limit_never_evicts() {
        let store = Store::new();
        for i in 0..1000 {
            store.set(format!("k{i}"), vec![0u8; 100]);
        }
        assert_eq!(store.len(), 1000);
    }

    #[test]
    fn staying_under_maxmemory_never_triggers_eviction() {
        let store = Store::with_eviction(1_000_000, Policy::Lru);
        for i in 0..10 {
            store.set(format!("k{i}"), vec![0u8; 10]);
        }
        assert_eq!(store.len(), 10);
    }

    #[test]
    fn lru_evicts_the_least_recently_touched_key_first() {
        // Each key ("k0".."k4") costs 2 (key) + 10 (value) = 12 bytes.
        // Cap of 40 bytes fits at most 3 keys comfortably.
        let store = Store::with_eviction(40, Policy::Lru);
        for i in 0..3 {
            store.set(format!("k{i}"), vec![0u8; 10]);
        }
        // Recency order so far, least to most recent: k0, k1, k2.
        store.set("k3".to_string(), vec![0u8; 10]);
        // k0 should have been evicted to make room.
        assert_eq!(store.get("k0"), Ok(None));
        assert_eq!(store.get("k1"), Ok(Some(vec![0u8; 10])));
        assert_eq!(store.get("k2"), Ok(Some(vec![0u8; 10])));
        assert_eq!(store.get("k3"), Ok(Some(vec![0u8; 10])));
    }

    #[test]
    fn lru_protects_a_recently_read_key_from_eviction() {
        let store = Store::with_eviction(40, Policy::Lru);
        store.set("k0".to_string(), vec![0u8; 10]);
        store.set("k1".to_string(), vec![0u8; 10]);
        store.set("k2".to_string(), vec![0u8; 10]);
        // Touch k0 again, making it the most recently used - k1 is now
        // the least recently used instead.
        store.get("k0").unwrap();
        store.set("k3".to_string(), vec![0u8; 10]);

        assert_eq!(
            store.get("k1"),
            Ok(None),
            "k1 should have been evicted, not k0"
        );
        assert_eq!(store.get("k0"), Ok(Some(vec![0u8; 10])));
    }

    /// The demonstration this stage's README promises: the same access
    /// pattern (bulk-load several keys, hammer one "hot" key with reads,
    /// then a one-off scan touches every other key once, then a write
    /// pushes the store over its cap) leaves LRU and LFU evicting
    /// different keys, because they're tracking different things.
    #[test]
    fn lru_and_lfu_diverge_under_a_scan_after_hot_reads() {
        let build = |policy| {
            // "hot" (3) + 10 = 13 bytes; each "scan-N" (6) + 10 = 16
            // bytes. Four keys total: 13 + 16*3 = 61 bytes. A cap of 70
            // fits all four comfortably, so nothing gets evicted during
            // setup — critical for this test: if eviction fired mid-build,
            // "hot" would still be tied in frequency/recency with
            // whatever hadn't been touched yet, making the outcome
            // depend on HashMap iteration order instead of the actual
            // access pattern this test means to exercise. (Found by a
            // flaky test run: an earlier, smaller cap let exactly that
            // happen.)
            let store = Store::with_eviction(70, policy);
            store.set("hot".to_string(), vec![0u8; 10]);
            store.set("scan-0".to_string(), vec![0u8; 10]);
            store.set("scan-1".to_string(), vec![0u8; 10]);
            store.set("scan-2".to_string(), vec![0u8; 10]);
            for _ in 0..20 {
                store.get("hot").unwrap();
            }
            // The "scan": touch every non-hot key once, most recently.
            store.get("scan-0").unwrap();
            store.get("scan-1").unwrap();
            store.get("scan-2").unwrap();
            store
        };

        // "new-key" (7) + 10 = 17 bytes; 61 + 17 = 78, over the 70 cap
        // by 8 - evicting any single 16-byte "scan-*" key is enough to
        // land back under it, so exactly one eviction happens here.
        let lru = build(Policy::Lru);
        lru.set("new-key".to_string(), vec![0u8; 10]);
        // Under LRU, "hot" is now the *least* recently used (the scan
        // touched everything else more recently), so it gets evicted
        // despite being read 20 times.
        assert_eq!(
            lru.get("hot"),
            Ok(None),
            "LRU should have evicted 'hot' after the scan"
        );

        let lfu = build(Policy::Lfu);
        lfu.set("new-key".to_string(), vec![0u8; 10]);
        // Under LFU, "hot" survives - it has far higher frequency than
        // any once-touched scan key.
        assert_eq!(
            lfu.get("hot"),
            Ok(Some(vec![0u8; 10])),
            "LFU should have protected 'hot' despite the scan"
        );
    }

    #[test]
    fn eviction_gives_up_gracefully_when_a_single_value_alone_exceeds_maxmemory() {
        let store = Store::with_eviction(5, Policy::Lru);
        // "k" (1) + 100 bytes = 101, already over a 5-byte cap on its
        // own - nothing to evict that would ever bring it under, so
        // this must return rather than loop forever.
        store.set("k".to_string(), vec![0u8; 100]);
        assert_eq!(store.get("k"), Ok(Some(vec![0u8; 100])));
    }
}
