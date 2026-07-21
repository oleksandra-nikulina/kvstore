//! The shared, thread-safe key-value store — now with per-key expiry.
//!
//! Expiry is enforced two ways, matching real Redis: **lazily**, every
//! method that touches a key checks whether it's expired and, if so,
//! removes it and treats it as absent right there — no separate pass
//! needed for a key that's actively being read; and **actively**, via
//! [`Store::sweep_expired`], which the caller is expected to invoke on a
//! timer from a background thread (wired up in `lib.rs`) so that a key
//! nobody ever reads again still eventually leaves memory.

use std::collections::HashMap;
use std::sync::Mutex;
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

pub struct Store {
    data: Mutex<HashMap<String, Entry>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        match data.get(key) {
            Some(entry) if entry.is_expired(now) => {
                data.remove(key);
                None
            }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }

    /// A full overwrite, so — same as real Redis's plain `SET` — any TTL
    /// the key previously had is cleared along with the old value.
    pub fn set(&self, key: String, value: Bytes) {
        self.data.lock().unwrap().insert(
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

    /// Sets `key`'s remaining lifetime to `ttl` from now. A `ttl` of zero
    /// (or, from a negative `EXPIRE` argument clamped to zero by the
    /// caller) makes the key expire immediately — no special-casing
    /// needed, since "expires_at <= now" is exactly what `is_expired`
    /// already checks. `ttl` comes straight from a client-supplied
    /// integer, so it can be enormous; `Instant + Duration` panics on
    /// overflow, so this uses `checked_add` and reports that case
    /// explicitly instead of taking down the connection thread.
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

    /// `None` if the key doesn't exist (or just expired); `Some(None)`
    /// if it exists but has no TTL; `Some(Some(remaining))` otherwise.
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

    /// Clears `key`'s TTL, if it has one. Returns whether a TTL was
    /// actually removed (matching `PERSIST`'s reply semantics: `false`
    /// for a missing key, a persistent key, or an already-expired key).
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

    /// One active-expiry pass: drops every key whose TTL has elapsed,
    /// whether or not anything has read it since. Meant to be called
    /// periodically from a background thread — see `lib.rs::run`.
    pub fn sweep_expired(&self) {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();
        data.retain(|_, entry| !entry.is_expired(now));
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

        // Lazily: the very next read observes it as gone.
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

    /// `sweep_expired` is what stands in for stage 4-6's lazy-only
    /// expiry keeping a never-read key around forever: this proves an
    /// expired key is dropped by the sweep alone, with no `get`/`ttl`
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
}
