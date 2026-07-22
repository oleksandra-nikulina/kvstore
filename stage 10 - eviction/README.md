# Stage 10 — eviction

Until now the store has grown without bound. This stage adds an
optional, configurable approximate memory cap (`Store::with_eviction`,
CLI: `--maxmemory <bytes> --policy lru|lfu`) and an eviction policy —
LRU (least recently used) or LFU (least frequently used) — that kicks in
once the cap is exceeded, removing keys to make room for new writes
instead of growing forever or rejecting writes outright. `Store::new()`
(no `--maxmemory` given) is unaffected — every earlier stage's behavior,
and every test carried forward from them, still holds with eviction
simply never triggering.

**LRU is a real O(1) data structure — an arena-indexed doubly linked
list (`src/eviction.rs`), not raw pointers or `Rc<RefCell<_>>`.** Plain
`usize` indices into a `Vec` stand in for pointers: the standard safe-Rust
answer to "I want an intrusive doubly linked list" without `unsafe`.
LFU is a simpler `HashMap<String, u64>` with an O(n) scan to find the
minimum on eviction — a deliberate scope asymmetry, named as such in the
code, not an oversight: real Redis's actual O(1) LFU needs a
frequency-bucketed structure this stage doesn't take on.

**The recency/frequency tracker lives behind its *own* lock, separate
from `Store`'s `RwLock`.** Recording an access has to happen on every
`GET`, and every such recording is itself a mutation (LRU move-to-front,
or an LFU counter bump) — there's no "pure read" version. Folding that
into `Store`'s own lock would mean every `GET` needs exclusive write
access again, undoing the entire point of the stage 4-9 `RwLock`
retrofit. So eviction bookkeeping is decoupled into its own `Mutex` —
and unlike `Store`, `Mutex` is the *correct* choice there, not just the
simpler one: every operation on the tracker mutates, so `RwLock` would
buy nothing. That decoupling has one real, narrow, deliberately-accepted
consequence (a key deleted concurrently with an in-flight access
recording can transiently "revive" a stale tracker entry) — handled
defensively where eviction actually happens (`Store::maybe_evict`)
rather than by re-coupling the locks.

**Two real bugs, found by tests failing, not by inspection — worth
naming since both were about the same root cause: excluding a candidate
*after* it's already been selected is a different, weaker guarantee
than excluding it *from selection in the first place*.**
1. A `SET` of a value that alone exceeds `maxmemory`, on a
   freshly-touched key, would immediately evict *that same key* — it
   was briefly the only (and therefore "least recently used") entry the
   LRU tracker knew about — silently discarding the write the client
   just asked for.
2. Fixing that by "give up if the tracker ever offers back the key that
   triggered this" was still wrong for LFU specifically: a brand-new key
   starts at the *lowest possible frequency*, so it's often the very
   *first* candidate offered, not the last — "give up on first contact"
   meant no eviction happened at all, most of the time, under LFU.

Both fixed the same way: `Eviction::evict_except(protect)` excludes the
protected key from candidate selection entirely (for LRU, walking past
it in the list; for LFU, filtering it out of the min-frequency scan),
so it's simply never a candidate rather than being selected and then
un-selected.

**Demonstrates:** an arena/index-based intrusive linked list as the
safe-Rust O(1) answer to a data structure that traditionally needs
`unsafe`; a second, independent lock existing specifically because one
operation (recording an access) has no non-mutating form; and the
behavioral difference between LRU and LFU under a genuinely
demonstrative access pattern — a handful of reads on one "hot" key, then
a one-off scan touching several other keys once each, then a write that
forces an eviction: LRU evicts the hot key (the scan touched everything
else more recently), LFU protects it (the scan never came close to its
access count).

**Run:** `cargo run -- <listen_port> [aof_path] --maxmemory <bytes>
--policy lru|lfu`, then write past the cap and observe which keys get
evicted. Omit `--maxmemory` for the original, uncapped behavior.

**Tests:** unit tests for the eviction data structures' touch/evict
behavior in isolation (including the LRU-vs-LFU divergence under the
scan-after-hot-reads pattern, and the two bugs above as explicit
regression tests), `Store`-level tests for approximate byte accounting
across every mutating command, and integration tests driving the same
LRU-vs-LFU demonstration over real sockets.
