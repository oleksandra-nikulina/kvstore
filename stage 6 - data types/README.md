# Stage 6 — data types

Everything so far has stored a single value type (bytes) per key. This
stage adds Redis's core collection types on top of the same store and
locking model: Lists (`LPUSH`, `RPUSH`, `LRANGE`, `LPOP`), Hashes
(`HSET`, `HGET`, `HGETALL`, `HDEL`), and Sets (`SADD`, `SREM`,
`SMEMBERS`, `SISMEMBER`).

The interesting design question isn't the commands themselves — it's how
a single `HashMap<String, Value>` represents multiple value shapes at
once (an enum: `Value::Bytes`, `Value::List`, `Value::Hash`,
`Value::Set`), and what happens when a command is used against the wrong
type (`LPUSH` on a key holding a plain string) — Redis returns a
`WRONGTYPE` error, and this stage does too.

**Demonstrates:** modeling heterogeneous value types behind one map with
an enum instead of separate maps per type (closer to how Redis's own
`robj` type-tagging works), and type-checking commands against stored
values before mutating.

**Run:** `cargo run -- <listen_port>`, then `redis-cli -p <listen_port>
LPUSH mylist a b c` / `LRANGE mylist 0 -1`.

**Tests:** unit tests per collection type's ops, `WRONGTYPE` error cases,
and integration tests over RESP for each new command family.
