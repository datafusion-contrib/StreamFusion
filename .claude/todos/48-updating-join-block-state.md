# Updating join: block-based state store (columnar blocks + row refs)

**Status:** TODO (2026-07-04 profiling round,
`.claude/research/nexmark-operator-profiles-2026-07.md`, lever 7). This is the follow-through on
the divergences/08 residue ("the per-row state store — a fresh `OwnedRow` per buffered row where
Flink reuses pooled `BinaryRowData`"; a free-list allocator was named as the next lever — the
reference engines point at a better one).

**Measured:** the updating joiner is 4-7 CPU samples/iteration on q9/q7/q4/q20 (islands) and its
per-row costs are the two `.owned()` heap rows per input row (key + payload) into
`HashMap<OwnedRow, HashMap<OwnedRow, RowMeta>>`, plus hashing. q3/q20 sit at 0.60-0.82x on the
generator (their remaining deficit is split between this store, the forked prefix — ticket 45 —
and the entry transpose); Alibaba's Java engine reports 5.8x on q20, so this family is where the
5-10x target lives.

**Reference model (Proton `RefCountDataBlockList.h` + `RowRefs.h`, RisingWave `CompactedRow`):**
- Retained side = a list of **big columnar blocks** (incoming small batches concatenated up to a
  block-size target — Proton's `pushBackOrConcat`). We already receive Arrow batches; state
  becomes "append batch to block list" with zero per-row copies.
- Hash table maps join key → list of `{block id, row index}` refs (Proton arena-allocates ref-list
  nodes in groups of 7; RisingWave's `JoinRowSet` is a Vec up to 4 entries before promoting to a
  BTreeMap — our fanouts are usually small).
- Retract/PK-override removes a ref; blocks are freed **whole** when their live-ref count reaches
  zero (watermark/TTL eviction also drops whole blocks). No per-row deallocation.
- Emit gathers matched rows by `take(block, indices)` — vectorized, no per-row decode (same
  direction as ticket 46's decode-free emit).
- Memory accounting gets simpler and more accurate: state bytes = live blocks' Arrow footprint.

Watch-outs: outer-join `num_assoc` bookkeeping (RisingWave keeps it as a separate integer "degree"
per row — a plain `Vec<i64>` parallel to the refs, mutated in place); the DataFusion hash-join
delegation for the probe (we buffer + rebuild probe-side batches anyway, so block state feeds it
directly); state restore (snapshot serializes blocks as IPC — cheaper than per-row rows today).

Acceptance: joiner islands show no per-row `owned()`/malloc in top leaves; q3/q20 ≥1x and q9/q4/q7
step up on the generator matrix; join parity suite (INNER/LEFT/RIGHT/FULL/SEMI/ANTI + residual +
retract) green; memory-accounting tests updated to the block model.
