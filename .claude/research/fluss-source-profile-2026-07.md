# Fluss source profile (2026-07-06)

CPU profiles (async-profiler 4.x, 35s steady-state samples via `asprof -e cpu -o collapsed`)
of the native Fluss rung, taken with `NexmarkMatrixBenchmark#flussNativeProfileLoop`
(`SF_PROFILE_FLUSS=true`, release build, `mimalloc,fluss` features, 500K events per iteration):
q0 native (the source-bound shape), and q15 native vs q15 stock (the differential for the
rung's worst cell, 0.78×). Numbers below are sample counts out of ~5.5–8.5K total per capture;
JIT-compilation and idle/park frames excluded from the analysis.

## Finding 0: the batch-coalescing hypothesis is refuted

A rows-per-drained-batch histogram (temporary instrumentation in `NativeFlussSplitReader`)
over 94 profile iterations: **500K rows arrive in ~6 batches, average 83,333 rows, most in the
128K–256K bucket** (plus the 1-row sentinel). fluss-rs's `RecordBatchLogScanner` returns
per-fetch batches, not per-producer-wire-batch slivers — and the preload's writer batches are
large anyway. The roadmap's "coalesce small scanner batches" follow-up was reasoned from the
PR-review observation that one `ScanBatch` maps to one wire batch; measured reality is the
opposite extreme. There is nothing to coalesce; if anything the island sees unusually *large*
batches on this rung.

## Finding 1: the native source itself is lean — its only real cost is zstd

q0 native (source → calc → sink transpose, 2.83×): the fluss-rs consume shows **no hotspots
beyond ZSTD decompression** (`ZSTD_decompressBlock/Sequences/Huf…`, ~260 samples) — the Fluss
ARROW log is zstd-framed on the wire, and both engines pay that decode in their clients. The
remaining native-side q0 time is the measurement perimeter shared by both engines: the
`ArrowToRowData` vector reads and `toChangelogStream`'s RowData→external conversion
(`RowRowConverter.toExternal`, `LocalDate.ofEpochDay`, UTF-8 decode — ~700 samples). Poll,
subscribe, FFI export/import, and the ns normalization cast are all noise-level. **The source
is not the optimization target.**

For contrast, the stock connector's source is *not* free: its per-row
`ColumnarRow`→Flink-`RowData` conversion (`FlussRowToFlinkRowConverter`, deep field getters,
`Long.valueOf` boxing, `TimestampNtz.toLocalDateTime`) is ~580 samples in the q15 stock
capture — that gap is exactly why the stateless queries hit 2.8× on this rung.

## Finding 2: the trailing queries are bound by the changelog aggregate's allocation churn

q15 native (GROUP BY day, multi-DISTINCT; 0.78×) hot list, next to q15 stock:

| Cost center (native) | ~samples | Stock counterpart | ~samples |
|---|---|---|---|
| `GroupAggregator::update` incl. `DistinctSet::add_i64` | ~400 | MurmurHash + `BinaryRowData.equals` state probes | ~475 |
| `RawVec::grow_one` → `memmove` (growing Vecs in the update/emit path) | ~230 | — | |
| mimalloc arena purge (`posix_madvise` in alloc + segment purge) | ~170 | — | |
| changelog emit via `ScalarValue::iter_to_array` (+ ScalarValue drops) | ~170 | — | |
| `DateFormat` UDF: `StrftimeItems` re-parsed per call + per-row `String` | ~90–130 | codegen'd formatter | (small) |
| sink perimeter (shared) | ~450 | sink perimeter (shared) | ~550 |

The native aggregate's *probe* work is comparable to Flink's (typed distinct sets vs murmur
hashing — a wash). What Flink does not pay is the **allocation column**: Vec growth memmove,
mimalloc returning purged segments to the OS (`madvise` syscalls — large transients cycling
per 83K-row batch), the emit path still materializing per-row `ScalarValue`s into arrays, and
the datetime formatter re-parsing its format string and allocating a `String` per row. This is
the same differential signature the 2026-07 operator round kept finding (native spends 10–22%
in the allocator where Flink spends ~1%) — surfaced here because the stock side's source
conversion advantage is gone (finding 1) and the whole cell is decided inside the aggregate.

## Ranked levers

1. **Group-aggregate emit + update allocation churn**: replace the emit's
   `ScalarValue::iter_to_array` with typed builders (the pattern the windowed aggregates and
   Top-N already moved to), and pre-size/reuse the update path's growing Vecs. Acceptance:
   q15/q16/q17 on the Fluss rung (currently 0.78×/0.85×/0.84×).
2. **`DATE_FORMAT` compile-once + builder writes**: pre-parse `StrftimeItems` at expression
   compile (the compile-once principle divergences/07 already applies to regex) and write
   formatted output straight into a `StringBuilder`-style Arrow builder instead of a per-row
   `String`. Benefits every `DATE_FORMAT` query (q10/q15/q16/q17) on every rung.
3. **mimalloc purge tuning**: `posix_madvise` cycling suggests eager segment purge under the
   giant-batch alloc/free rhythm; try `MIMALLOC_PURGE_DELAY` (or option_purge_delay) before
   structural changes — cheap experiment, measure on q15.
4. **Not the source**: fluss-rs consume is zstd-bound; both engines pay it. No coalescing
   (finding 0). Leave the reader alone until an operator-side lever moves the trailing cells.
   Measured follow-up: compression off (`table.log.arrow.compression.type = NONE`) makes q0
   native *slower* (2.83× → 2.18×; stock unchanged; q15 within noise) — fetches are byte-capped,
   so the uncompressed log costs ~4× the fetch round-trips. The zstd decode is the price of
   fewer RPCs, not removable overhead.
