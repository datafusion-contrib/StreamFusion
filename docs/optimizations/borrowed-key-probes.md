# Borrow bytes, copy only on first insert

**Applies to:** the updating join's two state levels, the keyed exchange's key-group assignment,
the GROUP BY aggregate's state map, changelog normalize, the keyed `OVER` loops, the retracting
Top-N, keep-first dedup, keep-last dedup, and the append-only Top-N's partition map

Every keyed native operator follows the same discipline: state maps key by `ByteKey` (`Box<[u8]>`
with `Borrow<[u8]>`), so the per-row probe hashes the *borrowed* encoded bytes straight out of the
batch's `Rows` block, and a key already in the map allocates nothing. Bytes are copied exactly once
— when a key, group, or partition first appears — not on every row that happens to match it. This
is the same borrowing discipline described on the [arrow-row byte state](arrow-row-state.md) page,
applied specifically to the *probe* side of every keyed map.

## Updating-join state probes borrow their bytes

Both sides' state maps key by raw arrow-row bytes (`ByteKey`, `Borrow<[u8]>`), so the per-row probe
hashes the borrowed encoded key/row and allocates only when a key or distinct row first enters
state — previously every input row paid two `OwnedRow` heap copies whether or not it was already
stored, the system-allocator signal the differential profile flagged vs. Flink's pooled
`BinaryRowData`. Emit/snapshot reconstruct rows from stored bytes via the converter's parser (wire
format unchanged).

q20 gained +4% on the generator loop. The Proton-style block store (state as columnar blocks + row
refs, emit by `take`) was rejected on that post-round profile — the stored-row decode no longer
registers — recorded in
[`.claude/wontdos/48-updating-join-block-state.md`](../../.claude/wontdos/48-updating-join-block-state.md).

## Flink BinaryRow keys encode per batch, probe borrowed

The operators that must key by Flink's `BinaryRowData` bytes rather than arrow-row — the keyed
exchange's key-group assignment, the GROUP BY aggregate's state map, changelog normalize — encoded
per row: each row re-walked the key type schema, allocated a fresh writer buffer, re-downcast every
key column, and copied the result into an owned map key even when the key was already in state. A
2026-07-12 profile put the path at ~12% of q17's whole job, almost all of it allocator traffic
rather than encoding.

A per-batch encoder now sets up the schema walk, column handles, and row buffer once, writes each
row into the reused buffer, and the group-aggregate/normalizer probes borrow the bytes, owning a
key only on first insert — the same discipline the updating join already had.

Measured with the transpose string fix on the 2M-event generator rung, every keyed query gained:

- q20 **+28%**
- q16 **+25%**
- q9 **+22%**
- q17 **+21%**
- q18 **+16%**
- q23 **+14%**
- q4 **+13%**
- q11 **+12%** native throughput

q16 went 1.03x → 1.27x vs. Flink, q9 went 1.07x → 1.32x, q20 went 0.82x → 0.99x. The Kafka
full-native rung's keyed cells moved +4 to +24 points.

## The ScalarValue-vintage keyed loops retired

The last operators still building a `Vec<ScalarValue>` key (or whole row) per input row moved to
the same arrow-row byte state as the rest: all three keyed `OVER` loops (running fold, bounded-frame
buffers, `ROW_NUMBER`/`RANK` counters) probe by borrowed key bytes; the **retracting Top-N** adopted
the append-only ranker's whole structure — memcomparable sort-key bytes replace the scalar
comparator, `Arc`-shared payload rows make the per-row before/after top-N snapshots refcount bumps
instead of row deep-clones, and the shared distinct-row emit decode applies; keep-first dedup's
emitted-key set probes borrowed bytes; and the exchange split hashes each row's encoded key bytes
from one vectorized pass.

Criterion (4096-row batches, 64–256 keys):

- OVER running sum: 422 → 183 µs (**+121%** throughput)
- `ROW_NUMBER`: 342 → 131 µs (**+162%**)
- bounded frame: 688 → 452 µs (**+52%**)
- retracting Top-N: 10.2 → 3.1 ms (**+228%**)
- exchange split: 174 → 57 µs (**+208%**)
- keep-first probe: **+6%**

The exchange's concrete key→channel assignment changed with the hashed representation — permitted
by divergences/10 (co-location is the only contract).

Still scalar-keyed as of this writing, pending a bench that says they matter: the window Top-N
ranker and the temporal join — tracked in the
[perf backlog](https://github.com/datafusion-contrib/StreamFusion/issues/14).

## Deep dive: steady-state probes borrow, only first inserts copy

State maps key by `ByteKey` (`Box<[u8]>` with `Borrow<[u8]>`), so the per-row probe hashes the
*borrowed* encoded bytes straight out of the batch's `Rows` block and a key already in the map
allocates nothing — the bytes are copied exactly once, when a key/group/partition first appears.

Shipped for the updating join's two state levels first (q20 +4%, q23 +21% cumulative in the
2026-07-04 round), then extended to:

- the changelog GROUP BY's group map,
- the keep-last deduplicator, whose stored payload became `Arc<[u8]>` — the replacing row is copied
  once into state, and the `-U` *moves* the replaced payload out, so an ignored stale row now
  allocates nothing at all,
- the append-only Top-N's partition map, where a row dropped at rank > N allocates nothing.

The GROUP BY's emitted changelog keys are borrowed slices too, decoded once per batch.

The second stage measured q23 **+8.5%**, q18 **+5.4%**, q16 **+3.4%** on the generator profile
loop — and its q23 profile is what closed the block-store question
([`.claude/wontdos/48-updating-join-block-state.md`](../../.claude/wontdos/48-updating-join-block-state.md)):
stored-row decode no longer registers in the joiner at all.
