# Stage 10 — eviction

Until now the store has grown without bound. This stage adds a
configurable memory cap and an eviction policy — LRU (least recently
used) and LFU (least frequently used) — that kicks in once the cap is
hit, removing keys to make room for new writes instead of growing
forever or rejecting writes outright.

**Demonstrates:** tracking recency/frequency alongside the value itself
without a full rescan on every access (an intrusive doubly-linked list +
hashmap for O(1) LRU touch/evict is the classic approach), and the
behavioral difference between LRU and LFU under different access
patterns (a one-off burst of scans vs. genuinely hot keys).

**Run:** `cargo run -- <listen_port> --maxmemory <bytes> --policy
lru|lfu`, then write past the cap and observe which keys get evicted.

**Tests:** unit tests for the eviction data structure's touch/evict
behavior in isolation, and an integration test writing past `maxmemory`
and asserting the expected keys were evicted under each policy.
