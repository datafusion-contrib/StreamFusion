# Arrow-row byte state everywhere

**Applies to:** the append-only Top-N, keep-last dedup, the updating join, group-aggregate keys,
the windowed/session aggregators, and the two-phase local aggregate's per-batch key encoding

Found by Criterion micro-benchmarks and differential CPU profiles (native vs. Flink on the same
query), which repeatedly localized the gap to per-row allocation the JVM side avoids via pooled
`BinaryRowData`.

Operators originally kept state as `Vec<ScalarValue>` rows: a heap allocation per row, scalar-enum
hashing, deep clones through every cascade, and scalar-by-scalar rebuilds on emit. Following
RisingWave (value-encoded state, memcomparable keys) and Arroyo (arrow `RowConverter`), state moved
to arrow-row bytes: encode once per batch, compare/hash/move byte buffers, rebuild output in one
vectorized `convert_rows` pass.

Measured per operator:

- **Append-only Top-N** — q19 0.25x → 0.87x, a 3.5x operator speedup.
- **Keep-last dedup** — q18 0.50x → 1.06x.
- **Updating join** — q20 0.49x → 0.91x.
- **Group-aggregate keys** — q15/q16/q17 +10–22 points.
- **Windowed/session aggregators** — keyed tumbling 245 → 110 µs, a 2.2x speedup.

Mechanics are in the [memcomparable arrow-row state](#memcomparable-arrow-row-state) deep dive
below.

## Fast non-cryptographic hashing

Internal grouping/state maps use ahash (what Arrow and DataFusion use) instead of `SipHash` — the
keys are never exposed to untrusted callers, so collision resistance buys nothing. Tumbling
aggregation ran ~36% faster unkeyed / ~16% keyed; q15 went 0.77x → 0.99x once the GROUP BY and
DISTINCT sets switched (profiling had shown ~61% of that operator in hashing). Mechanics are in the
[hashing footprint](#reducing-the-hashing-footprint) deep dive below.

ahash later became the **crate-wide default**: the shared `HashMap`/`HashSet` aliases now resolve
to ahash, after a 2026-07 profiling round caught operators that had missed the explicit swap — the
keep-last dedup (q18) was spending ~35% of its island in `SipHash`; the alias swap cut that island's
CPU ~16% (11.6 → 9.8 samples/iteration) and closes the gap for every future operator by default,
with no per-operator opt-in required.

## Mini-batch local aggregate keys encode as one Arrow-row batch

The two-phase local half of aggregation previously rebuilt a `Vec<ScalarValue>` and hashed its
values for every row, even though its input is already an Arrow batch. Real batches now encode all
group keys in one Arrow-row pass, probe the state map by borrowed bytes, allocate only a first-seen
key, and retain that key's original Arrow row for a gathered flush. A pinned scalar path preserves
the cheaper behavior for streams of single-row physical batches, since encoding a whole `Rows` block
for one row is pure overhead.

Criterion kept the one-row case at 0.95 M rows/s while moving a 4096-row logical bundle from
roughly 17.8 to 46.7 M rows/s and a 50000-row bundle to 52.2 M rows/s.

On the balanced 5M-event q17 comparison, mini-batch throughput rose from 1.149 to 1.436 M rows/s
(+25%); a matching 25-second CPU profile completed 152 loops versus 127 before, removed
`ScalarValue::hash` entirely (223 samples to zero), and cut local update/flush samples from 526/235
to 362/176. The same change moved q15's enabled path from 1.535 to 1.857 M rows/s (+21%); its
mini-batched path is now slightly faster than immediate (1.02x) and 1.64x stock Flink with
mini-batching enabled.

## Memcomparable arrow-row state

All keyed native state is arrow's row format (`arrow::row::RowConverter`), in two flavors used for
two different jobs:

- **Memcomparable keys** — grouping keys, join equi-keys, Top-N sort keys. The row encoding is
  order-preserving, so `memcmp` on the bytes *is* the SQL comparison. For sort keys the per-column
  direction is baked into the encoding itself (`SortField::new_with_options` with
  `descending`/`nulls_first`), so a Top-N's entire ORDER BY — mixed ASC/DESC, per-column null
  placement — collapses to one byte compare with no comparator dispatch per column. `OwnedRow` is
  `Ord`/`Eq` by its bytes, so ordering, map lookup, and the full-row equality retraction needs are
  all byte operations.
- **Value-encoded payloads** — the full stored row (join state, Top-N buffer, dedup state). Not
  compared, just held and moved; decoded back to typed Arrow columns in one vectorized
  `convert_rows` pass per emit/snapshot, replacing the scalar-by-scalar array rebuild.

The lifecycle that makes this cheap:

- **Encode once per batch.** `encode_keys` converts a batch's key columns into a `Rows` block in
  one call. The per-batch grouping map then keys by *borrowed* `Row<'_>` views into that block
  (`ahash::HashMap<(start, end, Row<'_>), Vec<u32>>` in the window aggregators) — zero per-row
  allocation during grouping; bytes are materialized to an `OwnedRow` only once per *touched
  group*, not per row.
- **State maps own bytes, cascades move them.** The updating join's state is
  `HashMap<OwnedRow, HashMap<OwnedRow, RowMeta>>` — key bytes → row bytes → appear-count. The key
  and row are encoded once on push and *moved* into the map; INNER never reuses them after the
  match gather, so there is no defensive clone. Both sides share one key-converter config, so equal
  keys encode to equal bytes across the two inputs and probe is a byte-hash lookup. Outer-join null
  padding is a pre-encoded all-null row per side, built once at construction.
- **Share instead of clone where a row is emitted twice.** The with-rank Top-N cascade emits the
  same buffered row as a `-U` at one rank and a `+U` at the next; the payload is `Arc<OwnedRow>` so
  both emits are refcount bumps. A buffered Top-N row is just
  `(sort_key: OwnedRow, payload: Arc<OwnedRow>)`.
- **Free side effects.** NULL keys get a defined order (the encoding places them; `ScalarValue`
  `partial_cmp` did not), making flush order deterministic. Managed-memory accounting becomes exact
  for keys — the tracked footprint is literally the byte length. Snapshots are unchanged on the
  wire: stored keys decode back to typed columns, so the checkpoint format never learned about the
  encoding.
- **The uncommon path pays the decode.** A residual non-equi predicate needs real arrays; the
  associated rows are bulk-decoded in one `convert_rows` call per batch, never row-at-a-time.

The same byte-first discipline extends to the JVM upcall path for byte-parity builtins (host-exact
`LOWER`/`UPPER` and similar) — those methods take and return raw string bytes rather than
materializing a `java.lang.String` per row. That marshaller has its own set of profile-driven fixes
and is covered on its own page:
[Host-exact builtins via upcall](host-exact-builtins-upcall.md).

Why this matters: a differential profile (native vs. Flink, same query) showed native spending
10–22% of samples in the system allocator where Flink spends ~0.7% — Flink keys by bytes too
(`MurmurHashUtils` over pooled `BinaryRowData`), so the `Vec<ScalarValue>` representation was pure
overhead Flink never pays. The byte-state migration is what moved 10 of 18 generator Nexmark
queries to ≥1x.

## Reducing the hashing footprint

Two multiplied costs were cut independently: the cost of one hash, and the number of bytes hashed.

- **One hash:** Rust's default `SipHasher` is DoS-resistant, which buys nothing for keys that never
  leave the operator. Every state map (group map, `COUNT(DISTINCT)` value sets, both levels of the
  join state) uses ahash — the same hasher Arrow and DataFusion use internally. On q16 the profile
  had ~61% of the operator inside `sip::Hasher::write` + `ScalarValue::hash/eq`.
- **What gets hashed:** hashing a `Vec<ScalarValue>` walks the enum per column and re-hashes heap
  strings per row; hashing an `OwnedRow` is one contiguous byte slice. The arrow-row migration
  (above) is therefore also a hashing optimization — encode once per batch, then every map touch
  hashes bytes.
- **How often:** steady-state rows hit existing groups, so the group map is reached by `get_mut` and
  the key is cloned only on first insert; a row landing in multiple windows moves its key into the
  last window and clones only for the earlier ones — zero clones for tumbling.
