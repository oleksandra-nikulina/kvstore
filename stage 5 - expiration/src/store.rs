//! The shared, thread-safe key-value store — now with per-key expiry.
//!
//! Expiry is enforced two ways, matching real Redis: **lazily** on the
//! write path (`SET` clearing a TTL, `EXPIRE`/`PERSIST`/`DEL` touching
//! an already-expired key removes it right there, since those already
//! need exclusive access) and **actively**, via [`Store::sweep_expired`],
//! which the caller is expected to invoke on a timer from a background
//! thread (wired up in `lib.rs`) so a key nobody ever writes through
//! again still eventually leaves memory. The pure read path (`GET`,
//! `TTL`) does *not* remove an expired entry it happens to find — see
//! [`peek`] below for why.
//!
//! A single `RwLock`, not a `Mutex`: `GET`/`TTL` take a shared read
//! lock and can run concurrently with each other; only the mutating
//! methods need exclusive access. Worth being honest about the trade
//! this makes, not just asserting it's better — see `DESIGN_TRADEOFFS_NOTES.md`
//! at the project root for the full reasoning, in short: `RwLock` isn't
//! "cheaper" than `Mutex` (a bit more bookkeeping per call, actually),
//! its payoff is letting genuine concurrent reads run in parallel
//! instead of serializing, `std::sync::RwLock` makes no fairness
//! promise (a write could in principle be starved by a steady stream of
//! reads), and it does nothing for the deeper one-lock-over-the-whole-keyspace
//! ceiling — that needs sharding, which this project doesn't do.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub type Bytes = Vec<u8>;

struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|at| now >= at)
    }
}

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

/// Read-only lookup: treats an expired entry as absent **without
/// removing it**. This is what lets `get`/`ttl` take a shared
/// `RwLock::read()` guard (`&HashMap`, not `&mut`) instead of an
/// exclusive write guard — a genuinely non-mutating read path, not just
/// a relabeled write path. The trade-off: an expired key nobody ever
/// writes through again now only gets physically removed by the active
/// sweep (or a write-path operation that happens to touch it), not by
/// the read that first observes it as gone. Observably identical to a
/// client either way — a `GET` on it returns `None` regardless of
/// whether the map slot has actually been freed yet.
fn peek<'a>(data: &'a HashMap<String, Entry>, key: &str, now: Instant) -> Option<&'a Entry> {
    match data.get(key) {
        Some(entry) if !entry.is_expired(now) => Some(entry),
        _ => None,
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

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let data = self.data.read().unwrap();
        peek(&data, key, Instant::now()).map(|entry| entry.value.clone())
    }

    /// A full overwrite, so — same as real Redis's plain `SET` — any TTL
    /// the key previously had is cleared along with the old value.
    pub fn set(&self, key: String, value: Bytes) {
        self.data.write().unwrap().insert(
            key,
            Entry {
                value,
                expires_at: None,
            },
        );
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
            if let Some(entry) = data.remove(key)
                && !entry.is_expired(now)
            {
                removed += 1;
            }
        }
        removed
    }

    /// Sets `key`'s remaining lifetime to `ttl` from now. A `ttl` of zero
    /// (or, from a negative `EXPIRE` argument clamped to zero by the
    /// caller) makes the key expire immediately — no special-casing
    /// needed, since "expires_at <= now" is exactly what `is_expired`
    /// already checks. `ttl` comes straight from a client-supplied
    /// integer, so it can be enormous; `Instant + Duration` panics on
    /// overflow, so this uses `checked_add` and reports that case
    /// explicitly instead of taking down the connection thread.
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

    /// `None` if the key doesn't exist (or is expired); `Some(None)` if
    /// it exists but has no TTL; `Some(Some(remaining))` otherwise. Pure
    /// read path — see [`peek`].
    pub fn ttl(&self, key: &str) -> Option<Option<Duration>> {
        let data = self.data.read().unwrap();
        let now = Instant::now();
        peek(&data, key, now).map(|entry| entry.expires_at.map(|at| at.saturating_duration_since(now)))
    }

    /// Clears `key`'s TTL, if it has one. Returns whether a TTL was
    /// actually removed (matching `PERSIST`'s reply semantics: `false`
    /// for a missing key, a persistent key, or an already-expired key).
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

    /// One active-expiry pass: drops every key whose TTL has elapsed,
    /// whether or not anything has read or written it since. Meant to be
    /// called periodically from a background thread — see
    /// `lib.rs::run`. This is the primary cleanup mechanism now that the
    /// read path (`get`/`ttl`) no longer removes expired entries itself
    /// — see [`peek`].
    pub fn sweep_expired(&self) {
        let mut data = self.data.write().unwrap();
        let now = Instant::now();
        data.retain(|_, entry| !entry.is_expired(now));
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

    #[test]
    fn get_on_a_missing_key_is_none() {
        let store = Store::new();
        assert_eq!(store.get("missing"), None);
    }

    #[test]
    fn set_then_get_round_trips_the_value() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        assert_eq!(store.get("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn set_overwrites_an_existing_value() {
        let store = Store::new();
        store.set("k".to_string(), b"first".to_vec());
        store.set("k".to_string(), b"second".to_vec());
        assert_eq!(store.get("k"), Some(b"second".to_vec()));
    }

    #[test]
    fn del_removes_present_keys_and_counts_only_those() {
        let store = Store::new();
        store.set("a".to_string(), b"1".to_vec());
        store.set("b".to_string(), b"2".to_vec());

        let removed = store.del(&["a".to_string(), "missing".to_string(), "b".to_string()]);

        assert_eq!(removed, 2);
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("b"), None);
    }

    #[test]
    fn concurrent_sets_to_the_same_key_never_produce_a_torn_value() {
        let store = Store::new();
        let writer_count = 32;

        thread::scope(|scope| {
            for i in 0..writer_count {
                let store = &store;
                scope.spawn(move || {
                    store.set("shared".to_string(), vec![i as u8; 64]);
                });
            }
        });

        let final_value = store.get("shared").expect("key should be present");
        assert!(
            final_value.iter().all(|&b| b == final_value[0]),
            "value contains a mix of bytes from different writers: {final_value:?}"
        );
        assert!((0..writer_count as u8).contains(&final_value[0]));
    }

    /// New in the RwLock migration: many threads concurrently *reading*
    /// the same key must all see a consistent value and never observe a
    /// torn/partial read, same guarantee `Mutex` gave, now via a shared
    /// read lock instead of exclusive access.
    #[test]
    fn concurrent_reads_of_the_same_key_are_all_consistent() {
        let store = Store::new();
        store.set("shared".to_string(), vec![7u8; 64]);
        let reader_count = 32;

        thread::scope(|scope| {
            for _ in 0..reader_count {
                let store = &store;
                scope.spawn(move || {
                    let value = store.get("shared").expect("key should be present");
                    assert_eq!(value, vec![7u8; 64]);
                });
            }
        });
    }

    #[test]
    fn ttl_on_a_missing_key_is_none() {
        let store = Store::new();
        assert_eq!(store.ttl("missing"), None);
    }

    #[test]
    fn ttl_on_a_key_with_no_expiry_is_some_none() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        assert_eq!(store.ttl("k"), Some(None));
    }

    #[test]
    fn expire_sets_a_ttl_on_an_existing_key() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        assert_eq!(store.expire("k", Duration::from_secs(60)), ExpireResult::Set);

        let remaining = store.ttl("k").unwrap().unwrap();
        assert!(remaining <= Duration::from_secs(60));
        assert!(remaining > Duration::from_secs(55));
    }

    #[test]
    fn expire_on_a_missing_key_does_nothing() {
        let store = Store::new();
        assert_eq!(
            store.expire("missing", Duration::from_secs(60)),
            ExpireResult::Missing
        );
    }

    #[test]
    fn a_zero_duration_expire_makes_the_key_immediately_expired() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        assert_eq!(store.expire("k", Duration::ZERO), ExpireResult::Set);

        // Lazily: the very next read observes it as gone (even though,
        // post-RwLock-migration, that read no longer physically removes
        // it — see `peek`'s doc comment).
        assert_eq!(store.get("k"), None);
    }

    #[test]
    fn persist_clears_an_existing_ttl() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_secs(60));

        assert!(store.persist("k"));
        assert_eq!(store.ttl("k"), Some(None));
    }

    #[test]
    fn persist_on_a_key_with_no_ttl_returns_false() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        assert!(!store.persist("k"));
    }

    #[test]
    fn persist_on_a_missing_key_returns_false() {
        let store = Store::new();
        assert!(!store.persist("missing"));
    }

    #[test]
    fn a_lazily_expired_key_is_invisible_to_get_ttl_persist_and_expire() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_millis(1));
        thread::sleep(Duration::from_millis(20));

        assert_eq!(store.get("k"), None);
        assert_eq!(store.ttl("k"), None);
        assert!(!store.persist("k"));
        assert_eq!(
            store.expire("k", Duration::from_secs(60)),
            ExpireResult::Missing
        );
    }

    /// The bug this test guards against: `EXPIRE`'s TTL is a
    /// client-supplied integer with no upper bound at the parsing layer
    /// (see `command.rs`), so it can be large enough that `now + ttl`
    /// would overflow `Instant`'s representable range. That must not
    /// panic the connection thread — it should report `Overflow`
    /// cleanly, leaving the key's existing state untouched.
    #[test]
    fn expire_with_a_ttl_that_would_overflow_instant_does_not_panic() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        assert_eq!(
            store.expire("k", Duration::from_secs(u64::MAX)),
            ExpireResult::Overflow
        );

        // The key is untouched: still present, still with no TTL.
        assert_eq!(store.get("k"), Some(b"v".to_vec()));
        assert_eq!(store.ttl("k"), Some(None));
    }

    #[test]
    fn del_does_not_count_an_already_expired_key() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_millis(1));
        thread::sleep(Duration::from_millis(20));

        assert_eq!(store.del(&["k".to_string()]), 0);
    }

    /// `sweep_expired` is what actually reclaims an expired key that's
    /// never read or written again post-expiry now that `get`/`ttl` no
    /// longer remove one they happen to find (see `peek`): this proves
    /// an expired key is dropped by the sweep alone, with no `get`/`ttl`
    /// call ever touching it.
    #[test]
    fn sweep_expired_removes_expired_keys_without_ever_being_read() {
        let store = Store::new();
        store.set("gone".to_string(), b"v".to_vec());
        store.expire("gone", Duration::from_millis(1));
        store.set("stays".to_string(), b"v".to_vec());

        thread::sleep(Duration::from_millis(20));
        store.sweep_expired();

        assert_eq!(store.len(), 1);
        assert_eq!(store.get("stays"), Some(b"v".to_vec()));
    }

    /// New in the RwLock migration: before the sweep (or any write-path
    /// operation) removes it, an expired key is still physically present
    /// in the map — `get`/`ttl` treat it as logically absent without
    /// removing it. Confirms that loosening is real (not accidentally
    /// still eager) and still fully invisible to callers regardless.
    #[test]
    fn an_expired_key_lingers_physically_until_a_write_path_operation_or_sweep_touches_it() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());
        store.expire("k", Duration::from_millis(1));
        thread::sleep(Duration::from_millis(20));

        // Several reads in a row: still logically gone every time, and
        // critically, reading it doesn't clean it up early.
        assert_eq!(store.get("k"), None);
        assert_eq!(store.get("k"), None);
        assert_eq!(store.ttl("k"), None);
        assert_eq!(store.len(), 1, "the expired entry should still physically occupy its map slot");

        store.sweep_expired();
        assert_eq!(store.len(), 0, "the sweep should be what actually removes it");
    }
}
