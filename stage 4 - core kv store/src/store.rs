//! The shared, thread-safe key-value store. A single `RwLock` around one
//! `HashMap` is the answer to "many connection threads need to read and
//! mutate the same data" — `GET` (the most common operation by far in a
//! typical KV workload) takes a shared read lock and can run concurrently
//! with other reads; `SET`/`DEL` take an exclusive write lock, same as a
//! `Mutex` would give them.
//!
//! Worth being honest about what this buys and doesn't: a `RwLock` isn't
//! simply "better" than a `Mutex` — uncontended, it's usually a little
//! *more* expensive per call (more internal bookkeeping to distinguish
//! shared vs. exclusive access), and `std::sync::RwLock` makes no
//! fairness guarantee, so a write could in principle be starved by a
//! steady stream of concurrent reads on some platforms. The payoff is
//! real concurrency between simultaneous reads instead of every `GET`
//! serializing behind every other `GET` — but it's still one lock over
//! the *entire* keyspace, so a `SET` to one key still blocks a `GET` of
//! a completely unrelated one either way. That deeper bottleneck is
//! what stage 11's benchmark is actually for; this is a smaller,
//! orthogonal improvement, not a fix for it.
use std::collections::HashMap;
use std::sync::{LockResult, RwLock};

pub type Bytes = Vec<u8>;

pub struct Store {
    data: RwLock<HashMap<String, Bytes>>,
}

/// Takes a lock guard even if the lock was poisoned by a panic in some
/// other critical section. `RwLock::read`/`write` return `Err`
/// specifically to warn that the shared data *might* be left
/// mid-mutation after a panic — but every critical section in this
/// store is a plain, infallible `HashMap` operation that can't actually
/// panic, so propagating that as a fresh panic here would only ever
/// cascade an unrelated failure into every other client's next request
/// too. Recovering and continuing is the safer default for a server
/// that should stay up for everyone else.
fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        recover(self.data.read()).get(key).cloned()
    }

    pub fn set(&self, key: String, value: Bytes) {
        recover(self.data.write()).insert(key, value);
    }

    /// Removes every key in `keys` that's present, returning how many
    /// actually existed (matches `DEL`'s reply semantics).
    pub fn del(&self, keys: &[String]) -> usize {
        let mut data = recover(self.data.write());
        keys.iter()
            .filter(|k| data.remove(k.as_str()).is_some())
            .count()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        recover(self.data.read()).len()
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
    fn get_and_set_still_work_after_the_lock_is_poisoned() {
        let store = Store::new();
        store.set("k".to_string(), b"v".to_vec());

        // Poison the lock by panicking while holding it.
        let _ = std::panic::catch_unwind(|| {
            let _guard = store.data.write().unwrap();
            panic!("simulated panic while holding the write lock");
        });

        // A cascading `.unwrap()` here would mean one unrelated panic
        // takes down every other client's next request too — recovery
        // means it doesn't.
        assert_eq!(store.get("k"), Some(b"v".to_vec()));
        store.set("k2".to_string(), b"v2".to_vec());
        assert_eq!(store.get("k2"), Some(b"v2".to_vec()));
    }

    /// Many threads racing to `SET` the *same* key concurrently. The
    /// `RwLock`'s exclusive write lock guarantees each individual `insert`
    /// is atomic, so after
    /// every thread finishes the key must hold exactly one of the
    /// written values in full — never a corrupted/torn mix of two.
    #[test]
    fn concurrent_sets_to_the_same_key_never_produce_a_torn_value() {
        let store = Store::new();
        let writer_count = 32;

        thread::scope(|scope| {
            for i in 0..writer_count {
                let store = &store;
                scope.spawn(move || {
                    // Distinct byte per writer, repeated, so a torn
                    // write (part of one value, part of another) would
                    // show up as a value with mixed bytes.
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

    /// Many threads each own a disjoint key and hammer set/get/del on it
    /// concurrently with everyone else hammering their own keys — proves
    /// unrelated keys don't interfere with each other under the shared
    /// lock, just that they serialize through it.
    #[test]
    fn concurrent_operations_on_disjoint_keys_dont_interfere() {
        let store = Store::new();
        let thread_count = 16;
        let rounds = 200;

        thread::scope(|scope| {
            for i in 0..thread_count {
                let store = &store;
                scope.spawn(move || {
                    let key = format!("key-{i}");
                    for round in 0..rounds {
                        store.set(key.clone(), vec![round as u8]);
                        assert_eq!(store.get(&key), Some(vec![round as u8]));
                    }
                    store.del(&[key]);
                });
            }
        });

        assert_eq!(store.len(), 0);
    }
}
