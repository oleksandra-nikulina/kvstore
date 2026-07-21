//! The shared, thread-safe key-value store. A single `Mutex` around one
//! `HashMap` is the simplest possible answer to "many connection threads
//! need to read and mutate the same data" — correct, and the obvious
//! bottleneck every later stage either lives with or works around
//! (stage 7's async rewrite still starts from this same shape).

use std::collections::HashMap;
use std::sync::Mutex;

pub type Bytes = Vec<u8>;

pub struct Store {
    data: Mutex<HashMap<String, Bytes>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.data.lock().unwrap().get(key).cloned()
    }

    pub fn set(&self, key: String, value: Bytes) {
        self.data.lock().unwrap().insert(key, value);
    }

    /// Removes every key in `keys` that's present, returning how many
    /// actually existed (matches `DEL`'s reply semantics).
    pub fn del(&self, keys: &[String]) -> usize {
        let mut data = self.data.lock().unwrap();
        keys.iter().filter(|k| data.remove(k.as_str()).is_some()).count()
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

    /// Many threads racing to `SET` the *same* key concurrently. The
    /// `Mutex` guarantees each individual `insert` is atomic, so after
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
