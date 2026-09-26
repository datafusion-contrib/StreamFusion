# Allocation discipline on the per-row paths

**Applies to:** windowed/session aggregation, [GROUP BY](../operators/group-by.md), [Top-N](../operators/top-n.md),
the updating join, `OVER`, deduplication, [Calc string encoding](../benchmarks/scalar-functions.md#hex)

Beyond the arrow-row and mini-batch mechanisms covered elsewhere, a series of targeted fixes removed
allocations and redundant per-row work from specific hot loops: reuse instead of realloc, move
instead of clone, batch instead of loop, and — where the access pattern actually fits — a columnar
kernel instead of a row loop at all.

## Per-row allocation cuts

- Reuse the per-row window buffer instead of allocating one per row (**26%** on tumbling, `3833e8d`).
- Move the grouping key into its last window instead of cloning it (**~18%** keyed, `ffec81e`).
- Reach existing groups by `get_mut` and clone the key only on insert (**~8%** on string keys,
  `6802752`).
- Defer owning a Top-N row until it is known to enter the buffer, and share the payload via `Arc` so
  the with-rank cascade's double emits are refcount bumps instead of row deep-clones (q19 0.76x →
  1.13x, q18 0.82x → 1.28x, `22f5c0f`).
- Move the key/row into join state instead of re-cloning it on insert (`c597142`).

## Batch the per-row folds

- The running `OVER` aggregate replaced a DataFusion update-batch-then-evaluate call per row with a
  small typed running state folded directly (**~2.6x**, `945d3da`).
- The INNER updating join gathers bounded chunks of candidate pairs, evaluates the residual predicate
  columnar in one pass, and emits by `filter_record_batch` — one convert/eval per chunk instead of
  per row (q9 0.39x → ~1.0x, `4429e2f`); associated rows in the residual path bulk-decode in one
  `convert_rows` call (q7 0.33x → 0.74x, `ed74dac`).
  Candidate chunks now stop at 4,096 rows or an 8 MiB decode/filter estimate, and flush earlier
  when the task reservation cannot grow. The JNI receiver imports each chunk synchronously,
  including mini-batch flush output; no complete fanout is reassembled. This bounds transient
  candidate memory even when a residual predicate rejects every pair. It does not bound retained
  state or residual-function scratch allocation. See [regular join](../operators/joins/regular-join.md#bounded-inner-output).
  A September 24, 2026 ARM64/JDK 17 release+mimalloc comparison used the unchanged
  `CrossJoinBenchmark` (100,000 input rows crossed with 16 rows, both transposes, row sink,
  two warmups and five interleaved host/native trials per build). The native median moved from
  0.203549 s on main to 0.216456 s with bounded output (+6.3% elapsed time); host medians were
  0.193179 s and 0.195739 s. This is a memory-robustness tradeoff, not a throughput improvement.
- The session aggregator segments each key's rows into gap-connected runs so a run pays one value
  slice and one accumulator update, with the merge scan a bounded O(log n) range probe (**9.4x** on
  dense sessions, `62dffda`).

## String encoding output buffers

### HEX

Integer HEX writes uppercase digits from a bounded 16-byte stack buffer, following Comet's integer encoding pattern. String HEX checks output sizes and writes uppercase digits directly into the final Arrow buffer, avoiding temporary strings and a separate uppercase array. Arrow's safe constructor validates the result.

Output offsets are checked against Arrow Utf8's 32-bit limit before allocating the final values buffer. NULL rows consume no bytes and reuse input validity.

[Per-function complete-job results](../benchmarks/scalar-functions.md#hex) include both transposes.

### TO_BASE64

Uses standard padded base64 encoding, computes output offsets with checked sizes, and writes directly into the final Arrow buffer. Unlike Spark's MIME form modeled by Comet, Flink does not wrap lines. Arrow's safe constructor validates the result.

Output offsets are checked against Arrow Utf8's 32-bit limit before allocating the final values buffer. NULL rows consume no bytes and reuse input validity.

[Per-function complete-job results](../benchmarks/scalar-functions.md#to_base64) include both transposes.

### UNHEX

Follows Comet's nibble lookup table and combined invalid-digit check, while retaining Flink's odd-length rule. Validation and decoding share a pass into the final Arrow binary buffer. Invalid rows roll back partial output. Capacity uses the active slice, offsets stay checked, and the constructor remains safe; no per-row temporary output copy is needed.

[Per-function complete-job results](../benchmarks/scalar-functions.md#unhex) include both transposes.

## Columnar-kernel internal state where it fits

Keep-first dedup holds its per-key candidates as a single Arrow batch — one row per pending key —
reduced per input batch with filter/take/concat kernels, reading only the key and the rowtime per
row, the same minimal per-row read Arrow's own hash aggregate does; it never boxes rows into scalars
(`ebfde70`).

This was deliberately **not** applied to window Top-N: bounded ranking with arrival-order
tie-breaking maps poorly onto columnar kernels, so its buffer stays row-oriented, as it does in
Arroyo and RisingWave.
