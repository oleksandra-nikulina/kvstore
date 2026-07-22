//! What key a client requests next. The three workloads exist to
//! isolate *one variable at a time* across the stage's comparisons:
//! `SetGet` has zero cross-client key overlap (a clean baseline for
//! comparing thread-per-connection vs. async, since lock contention
//! shouldn't be a factor there); `HotKeys` and `SpreadKeys` are the
//! *same* request pattern at two extremes of key overlap, specifically
//! to make lock-contention cost visible as a throughput/latency delta
//! between the two, not a theoretical concern.

use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub enum Workload {
    /// One fixed key per client, `SET` then `GET` in a loop. No two
    /// clients ever touch the same key, so this is the workload to use
    /// when the thing under test is the networking/concurrency model
    /// itself, not the store's locking.
    SetGet,
    /// Every client draws from the same small pool of keys — heavy
    /// contention on the store's single global lock by construction.
    HotKeys,
    /// Every client draws from a pool large enough that two clients
    /// colliding on the same key is a rounding error — effectively the
    /// no-contention case, for the same request shape as `HotKeys`.
    SpreadKeys,
}

pub const HOT_KEY_COUNT: u64 = 8;
pub const SPREAD_KEY_COUNT: u64 = 1_000_000;

impl FromStr for Workload {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "set-get" => Ok(Workload::SetGet),
            "hot-keys" => Ok(Workload::HotKeys),
            "spread-keys" => Ok(Workload::SpreadKeys),
            other => Err(format!(
                "unknown workload '{other}' (expected set-get, hot-keys, or spread-keys)"
            )),
        }
    }
}

impl Workload {
    pub fn name(&self) -> &'static str {
        match self {
            Workload::SetGet => "set-get",
            Workload::HotKeys => "hot-keys",
            Workload::SpreadKeys => "spread-keys",
        }
    }

    /// The key this client should use for its next request. `rng` is
    /// each client's own PRNG state, advanced in place.
    pub fn key_for(&self, client_id: usize, rng: &mut u64) -> String {
        match self {
            Workload::SetGet => format!("bench-client-{client_id}"),
            Workload::HotKeys => format!("bench-hot-{}", xorshift(rng) % HOT_KEY_COUNT),
            Workload::SpreadKeys => format!("bench-spread-{}", xorshift(rng) % SPREAD_KEY_COUNT),
        }
    }
}

/// A tiny, dependency-free PRNG (xorshift64) — plenty uniform for
/// picking a key out of a few million buckets, and avoids pulling in
/// the `rand` crate for what's otherwise this project's first and only
/// need for randomness.
pub fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}
