# Streaming Top-N: net-diff staging + decode-free emit

**Status:** TODO (2026-07-04 profiling round,
`.claude/research/nexmark-operator-profiles-2026-07.md`, lever 3).

**Measured:** q19's `pushTopNRanker` spends **~72% of its CPU decoding arrow-row state bytes back
into columns** (`arrow_row::variable::decode_binary` 621 + `fixed::decode_fixed` 328 +
`decoded_len` 192 + `decode_nulls` 104 + memmove 281, of 2119 island samples). The with-rank shift
cascade amplifies output (each entering row emits UPDATE_BEFORE/UPDATE_AFTER per shifted rank), and
every emitted row is decoded row-at-a-time from stored `OwnedRow` bytes; q19 also pays the largest
exit transpose (4.8 samples/iter) re-materializing that amplified stream to `RowData`. q19 sits at
1.31-1.44x native on the generator while Alibaba's Java engine reports 6.2x on it (mini-batch) —
this operator is our biggest single-query gap to the 5-10x target.

Two complementary directions (references):

1. **Net-diff staging (RisingWave `TopNStaging`, `top_n/top_n_cache.rs:827`).** Stage the shift
   cascade per input batch in a change buffer keyed by rank position; a row that enters and leaves
   the window within one batch cancels; adjacent -old/+new on the same rank collapse to one
   UPDATE pair. Emit the *net* change per batch. RisingWave's three-zone (low/middle/high) cache
   also bounds each insert/delete to one hop per zone boundary — our ranker's per-rank shift walk
   should compare against it. Parity note: Flink's `AppendOnlyTopNFunction` emits the full cascade
   per record; **mini-batch Flink does not** — batching the emit per Arrow batch is exactly what
   Flink's own mini-batch mode does, so parity holds against mini-batch output ordering (verify
   against the host with mini-batch on; the row-set is identical either way, ordering within the
   batch is the thing to pin).
2. **Decode-free emit (Proton block-state model, `RefCountDataBlockList.h`/`RowRefs.h`).** Keep
   (or additionally keep) the Top-N window's rows in columnar form — retained input blocks with
   {block, row} refs, or a small per-partition Arrow buffer — so emit is `take` by indices
   (vectorized) instead of per-row arrow-row decode. Proton GCs state block-granularly by
   refcount; our windows are small (N≤10s), so a per-partition micro-batch column store or even
   re-encoding the emitted batch once per flush from a decoded staging table may suffice.

Also within reach while in the file: the ranker's key map is one of the remaining SipHash/scalar
`GroupKey` loops (see ticket 20's dedup evidence — q18 spends 35% of its island in SipHash; the
Top-N bookkeeping map has the same shape).

Acceptance: q19/q18/q9 profile shows decode no longer dominant; q19 native ≥2x generator-rung
(parity suite + Top-N harness tests green, incl. rank projection, OFFSET, retracting input).
