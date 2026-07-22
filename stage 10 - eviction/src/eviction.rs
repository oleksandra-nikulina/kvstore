//! Tracks recency (LRU) or frequency (LFU) per key so `store.rs` can
//! pick an eviction victim once the store is over its configured
//! `maxmemory`.
//!
//! Deliberately a lock separate from `Store`'s own `RwLock`, not folded
//! into it. Recording an access has to happen on *every* touch of a
//! key — including a plain `GET` — and every such recording is itself a
//! mutation (an LRU move-to-front, or an LFU counter bump); there's no
//! "pure read" version of it. Bundling that into `Store`'s own lock
//! would mean every `GET` needs exclusive write access again, undoing
//! the whole point of the stage 4-9 `RwLock` retrofit (see
//! `DESIGN_TRADEOFFS_NOTES.md` at the project root). So this tracker
//! gets its own lock, updated independently of `Store`'s data lock —
//! and unlike `Store`, a plain `Mutex` is the *correct* choice here,
//! not just the simpler one: every single operation on this tracker
//! mutates, so a `RwLock` would buy nothing.
//!
//! That decoupling has one real, narrow consequence: a key could be
//! deleted from the store (by another thread) in the brief window
//! between a caller reading it and calling [`Eviction::record_access`]
//! for it, which would "revive" a tracker entry for a key that's
//! already gone. `store.rs`'s eviction loop handles this defensively —
//! see `Store::maybe_evict` — rather than trying to prevent it here,
//! since preventing it would mean re-coupling the two locks and losing
//! the reason this module exists.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    Lru,
    Lfu,
}

/// One arena slot in the intrusive-via-indices doubly linked list.
/// "Intrusive" in the classic sense (the list's prev/next live inside
/// the node, not in a separate structure) but with plain `usize`
/// indices into [`LruList::nodes`] standing in for pointers — the usual
/// safe-Rust answer to "I want a doubly linked list, with O(1) removal
/// from the middle." Real pointers here would need `unsafe`; `Rc<RefCell<_>>`
/// would be safe but adds reference-counting overhead this doesn't need,
/// since the arena (`Vec`) already owns every node outright.
struct Node {
    key: String,
    prev: Option<usize>,
    next: Option<usize>,
}

/// A doubly linked list of keys ordered by recency — head is
/// most-recently-used, tail is least — with O(1) move-to-front and O(1)
/// removal from any position, the two operations LRU needs on every
/// access. The `index` map is what makes "find this key's node" O(1)
/// too, instead of a linear scan down the list.
struct LruList {
    nodes: Vec<Node>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    index: HashMap<String, usize>,
}

impl LruList {
    fn new() -> Self {
        LruList {
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            index: HashMap::new(),
        }
    }

    fn unlink(&mut self, id: usize) {
        let (prev, next) = (self.nodes[id].prev, self.nodes[id].next);
        match prev {
            Some(p) => self.nodes[p].next = next,
            None => self.head = next,
        }
        match next {
            Some(n) => self.nodes[n].prev = prev,
            None => self.tail = prev,
        }
    }

    fn push_front(&mut self, id: usize) {
        self.nodes[id].prev = None;
        self.nodes[id].next = self.head;
        if let Some(h) = self.head {
            self.nodes[h].prev = Some(id);
        }
        self.head = Some(id);
        if self.tail.is_none() {
            self.tail = Some(id);
        }
    }

    /// Moves `key` to the front (most-recently-used), inserting it
    /// fresh if it isn't already tracked.
    fn touch(&mut self, key: &str) {
        if let Some(&id) = self.index.get(key) {
            self.unlink(id);
            self.push_front(id);
            return;
        }
        let id = match self.free.pop() {
            Some(id) => {
                self.nodes[id] = Node {
                    key: key.to_string(),
                    prev: None,
                    next: None,
                };
                id
            }
            None => {
                self.nodes.push(Node {
                    key: key.to_string(),
                    prev: None,
                    next: None,
                });
                self.nodes.len() - 1
            }
        };
        self.index.insert(key.to_string(), id);
        self.push_front(id);
    }

    /// Stops tracking `key`, if present — keeps this list from drifting
    /// out of sync with the store's actual contents when a key is
    /// removed some other way (`DEL`, expiry, eviction itself).
    fn remove(&mut self, key: &str) {
        if let Some(id) = self.index.remove(key) {
            self.unlink(id);
            self.free.push(id);
        }
    }

    /// Removes and returns the least-recently-used key, skipping
    /// `protect` if given, without disturbing anything else's order.
    /// Walking from the tail rather than just checking-then-giving-up
    /// on the tail matters: `protect` is normally the *most* recently
    /// used entry (the caller just touched it), so in the overwhelming
    /// common case this is still O(1) — the tail simply isn't `protect`
    /// and the loop stops on the first check. It only walks further in
    /// the rare case where `protect` itself is the sole remaining entry.
    fn evict_except(&mut self, protect: Option<&str>) -> Option<String> {
        let mut id = self.tail;
        while let Some(node_id) = id {
            if Some(self.nodes[node_id].key.as_str()) != protect {
                let key = self.nodes[node_id].key.clone();
                self.remove(&key);
                return Some(key);
            }
            id = self.nodes[node_id].prev;
        }
        None
    }
}

/// Frequency-per-key. Finding the minimum for [`LfuMap::evict`] is a
/// linear scan — O(n), unlike `LruList`'s O(1) — which is a deliberate
/// scope choice, not an oversight: real Redis's actual O(1) LFU uses a
/// frequency-bucketed structure (each distinct frequency gets its own
/// list, plus a pointer to the lowest non-empty bucket), which is real
/// additional complexity this stage doesn't take on. LRU gets the
/// "proper" O(1) structure as this stage's featured lesson; LFU gets a
/// simpler, still-correct version, and the asymmetry is named here
/// rather than left implicit.
struct LfuMap {
    counts: HashMap<String, u64>,
}

impl LfuMap {
    fn new() -> Self {
        LfuMap {
            counts: HashMap::new(),
        }
    }

    fn touch(&mut self, key: &str) {
        *self.counts.entry(key.to_string()).or_insert(0) += 1;
    }

    fn remove(&mut self, key: &str) {
        self.counts.remove(key);
    }

    /// Ties (multiple keys at the same minimum frequency) break however
    /// `HashMap` iteration happens to order them — unspecified, not
    /// meaningful, and not worth building a secondary tiebreak structure
    /// for at this stage's scope.
    ///
    /// Excludes `protect` from consideration entirely, rather than
    /// picking the true minimum and then checking — this matters far
    /// more here than in `LruList::evict_except`: a freshly-written key
    /// naturally starts at the *lowest* possible frequency, so it's
    /// often the very first (not last) candidate a naive "find the
    /// minimum" scan would offer. Found by a test failure: protecting
    /// after the fact meant giving up immediately instead of trying any
    /// other candidate at all.
    fn evict_except(&mut self, protect: Option<&str>) -> Option<String> {
        let victim = self
            .counts
            .iter()
            .filter(|(k, _)| Some(k.as_str()) != protect)
            .min_by_key(|&(_, &count)| count)
            .map(|(k, _)| k.clone())?;
        self.counts.remove(&victim);
        Some(victim)
    }
}

enum Tracker {
    Lru(LruList),
    Lfu(LfuMap),
}

pub struct Eviction {
    tracker: Mutex<Tracker>,
}

impl Eviction {
    pub fn new(policy: Policy) -> Self {
        let tracker = match policy {
            Policy::Lru => Tracker::Lru(LruList::new()),
            Policy::Lfu => Tracker::Lfu(LfuMap::new()),
        };
        Eviction {
            tracker: Mutex::new(tracker),
        }
    }

    /// Records that `key` was just accessed (read or written). See this
    /// module's doc comment for why this is a separate lock from
    /// `Store`'s own, and the narrow race that decoupling accepts.
    pub fn record_access(&self, key: &str) {
        match &mut *self.tracker.lock().unwrap() {
            Tracker::Lru(list) => list.touch(key),
            Tracker::Lfu(map) => map.touch(key),
        }
    }

    /// Stops tracking `key` — call whenever it's removed from the store
    /// by any means, so this tracker doesn't drift from the store's
    /// actual contents.
    pub fn forget(&self, key: &str) {
        match &mut *self.tracker.lock().unwrap() {
            Tracker::Lru(list) => list.remove(key),
            Tracker::Lfu(map) => map.remove(key),
        }
    }

    /// Picks, and stops tracking, the key that should be evicted next,
    /// never selecting `protect` — `None` if nothing *else* is
    /// currently tracked. See `store.rs::Store::maybe_evict` for why
    /// "exclude, not just check after the fact" is the part that
    /// actually matters here.
    pub fn evict_except(&self, protect: Option<&str>) -> Option<String> {
        match &mut *self.tracker.lock().unwrap() {
            Tracker::Lru(list) => list.evict_except(protect),
            Tracker::Lfu(map) => map.evict_except(protect),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_in_least_recently_used_order() {
        let e = Eviction::new(Policy::Lru);
        e.record_access("a");
        e.record_access("b");
        e.record_access("c");
        // Order so far, least to most recent: a, b, c.
        assert_eq!(e.evict_except(None), Some("a".to_string()));
        assert_eq!(e.evict_except(None), Some("b".to_string()));
        assert_eq!(e.evict_except(None), Some("c".to_string()));
        assert_eq!(e.evict_except(None), None);
    }

    #[test]
    fn lru_touching_an_existing_key_moves_it_to_most_recently_used() {
        let e = Eviction::new(Policy::Lru);
        e.record_access("a");
        e.record_access("b");
        e.record_access("c");
        e.record_access("a"); // a is now the most recently used again
        // Least to most recent: b, c, a.
        assert_eq!(e.evict_except(None), Some("b".to_string()));
        assert_eq!(e.evict_except(None), Some("c".to_string()));
        assert_eq!(e.evict_except(None), Some("a".to_string()));
    }

    #[test]
    fn lru_forget_removes_a_key_from_the_middle_of_the_list() {
        let e = Eviction::new(Policy::Lru);
        e.record_access("a");
        e.record_access("b");
        e.record_access("c");
        e.forget("b");
        assert_eq!(e.evict_except(None), Some("a".to_string()));
        assert_eq!(e.evict_except(None), Some("c".to_string()));
        assert_eq!(e.evict_except(None), None);
    }

    #[test]
    fn lru_evict_on_an_empty_tracker_is_none() {
        let e = Eviction::new(Policy::Lru);
        assert_eq!(e.evict_except(None), None);
    }

    #[test]
    fn lru_reuses_freed_slots_instead_of_growing_forever() {
        // Not directly observable from the public API, but exercising
        // many touch/evict cycles is exactly what would surface a bug
        // in the free-list reuse logic (e.g. a stale prev/next pointer
        // left over from a previous occupant of a reused slot).
        let e = Eviction::new(Policy::Lru);
        for round in 0..50 {
            for i in 0..10 {
                e.record_access(&format!("k{round}-{i}"));
            }
            for _ in 0..10 {
                e.evict_except(None);
            }
        }
        assert_eq!(e.evict_except(None), None);
    }

    #[test]
    fn lfu_evicts_the_least_frequently_used_key() {
        let e = Eviction::new(Policy::Lfu);
        e.record_access("hot");
        e.record_access("hot");
        e.record_access("hot");
        e.record_access("cold");
        assert_eq!(e.evict_except(None), Some("cold".to_string()));
        assert_eq!(e.evict_except(None), Some("hot".to_string()));
        assert_eq!(e.evict_except(None), None);
    }

    #[test]
    fn lfu_forget_stops_tracking_a_key() {
        let e = Eviction::new(Policy::Lfu);
        e.record_access("a");
        e.record_access("b");
        e.forget("a");
        assert_eq!(e.evict_except(None), Some("b".to_string()));
        assert_eq!(e.evict_except(None), None);
    }

    #[test]
    fn lfu_evict_on_an_empty_tracker_is_none() {
        let e = Eviction::new(Policy::Lfu);
        assert_eq!(e.evict_except(None), None);
    }

    /// The actual point of having two policies: the same access pattern
    /// (a handful of hits on "hot", then one touch each on many "scan"
    /// keys) leaves LRU and LFU disagreeing about what to evict next —
    /// LRU only remembers *recency*, so the scan's most recent touch
    /// beats "hot"'s many-but-less-recent ones; LFU remembers actual
    /// *frequency*, so "hot" survives the scan.
    #[test]
    fn lru_and_lfu_disagree_on_a_scan_after_hot_keys() {
        let lru = Eviction::new(Policy::Lru);
        let lfu = Eviction::new(Policy::Lfu);
        for tracker in [&lru, &lfu] {
            for _ in 0..10 {
                tracker.record_access("hot");
            }
            for i in 0..5 {
                tracker.record_access(&format!("scan-{i}"));
            }
        }

        // LRU: the scan touched every "scan-*" key more recently than
        // "hot", so "hot" is now the least recently used of all of them.
        assert_eq!(lru.evict_except(None), Some("hot".to_string()));

        // LFU: every "scan-*" key was only ever touched once; "hot" was
        // touched 10 times, so a "scan-*" key goes first instead.
        let lfu_victim = lfu.evict_except(None).unwrap();
        assert!(lfu_victim.starts_with("scan-"));
    }
}
