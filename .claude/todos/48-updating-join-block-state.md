# Updating join: block-based state store (columnar blocks + row refs)

**Status:** stage 1 SHIPPED 2026-07-04 — zero-allocation steady-state probes. The block-store
redesign below remains, now gated on a fresh profile showing the residual is worth it.

**Shipped (stage 1):** both state maps key by `ByteKey` (raw arrow-row bytes with `Borrow<[u8]>`),
so the per-row probes hash the *borrowed* encoded key/row and allocate only when a key or distinct
row is first inserted — previously every input row paid two `OwnedRow` heap copies (key + payload)
whether or not it was already in state, the system-allocator signal the differential profile
flagged vs Flink's pooled `BinaryRowData`. Emit and snapshot reconstruct rows from stored bytes
via the converter's parser; wire format unchanged. Measured: q20 +4% on the generator profile
loop (join island is a minority of that query's CPU; the columnar rungs see the operator's share
proportionally larger).

## What remains: the Proton-style block store

State as retained columnar blocks (incoming batches concatenated to a size target) indexed by
key → `{block, row}` ref lists; blocks freed whole by live-ref count; emit gathers matched rows by
`take`/`interleave` from blocks instead of arrow-row decode (`convert_rows`) of stored bytes.
References: Proton `RefCountDataBlockList.h`/`RowRefs.h` (arena ref-list nodes of 7,
`RowRefListMultiple` for retract erase), RisingWave `JoinRowSet` (Vec ≤4 → BTreeMap).

Worth building only if a post-stage-1 profile still shows the joiner bound by stored-row decode
(`convert_rows` in `push_inner`'s candidate gather) or by first-insert copies — check q23 (the
three-way join, the largest joiner island: 13.3 samples/iter pre-change) after the current
optimization round's full matrix rerun. Watch-outs recorded earlier: `num_assoc` bookkeeping as a
parallel degree vec, snapshot becomes block IPC (cheaper than today's per-row decode), restore
compatibility not required across builds.
