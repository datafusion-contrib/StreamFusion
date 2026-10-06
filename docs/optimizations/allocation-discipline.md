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

Decimal rescaling returns zero immediately for zero coefficients, rejects output precision
overflow before multiplication, and rounds fully discarded coefficients without materializing
a power of ten. Remaining powers are bounded by the input coefficient's digits or output
precision. This prevents short exponent strings from requesting exponent-sized temporary
integers; release CSV `decimal_exponents` fixtures exercise the production decode boundary
and report allocation requests. These fixtures validate bounded allocation behavior and do
not claim an end-to-end speedup.

## Final window output buffers

Final legacy group-window emission **without window properties** creates vectors from the declared
Flink output fields and moves unchanged
native key/result buffers into them with Arrow transfer pairs. This removes per-value Java copying
and avoids allocating destination buffers that would immediately be replaced. The imported native
flush root can close before downstream consumption; output vectors own the transferred buffers,
while declared field names and timestamp child metadata remain intact. This follows Comet's use of
native Arrow vectors as the backing storage of decoded output.

Narrowed integer keys and legacy nanosecond timestamp keys still allocate and convert their values.
Outputs with any window properties retain the original preallocated root, copies, and property
conversion loops; the broader transfer candidate was rejected after measured regressions. If shaping fails after some transfers, the partially populated output root
closes before the exception propagates. `WindowOutputOwnershipTest` checks buffer identities,
nullable/fractional values, declared schema, source closure, and allocation release after failure.
The three [window handoff benchmarks](../operators/window-aggregate.md#output-handoff-benchmarks)
separately measure output shaping, full native flush, and complete jobs against stock Flink.

The following historical measurements evaluate the rejected broader transfer candidate.
Matched-fixture measurements on JDK 17, Flink 2.2.1, and release native libraries are retained in
[raw samples and sharing evidence](../benchmarks/results/window-output-handoff.json). Each shaping
fixture has 20 warmups and 31 observations in separate baseline/candidate runs. At 16,384 output
rows, the median timings were:

| Shape | No properties, before → after | Two properties, before → after |
| --- | ---: | ---: |
| BIGINT | 0.457 → 0.293 ms | 9.728 → 9.212 ms |
| Nullable BIGINT | 0.313 → 0.206 ms | 9.634 → 8.875 ms |
| STRING | 1.184 → 0.238 ms | 9.634 → 9.380 ms |
| Narrowed INT | 0.318 → 0.371 ms | 8.923 → 11.655 ms |
| TIMESTAMP(9) | 0.651 → 0.242 ms | 9.763 → 12.174 ms |

Compatible result buffers shared their source address in all 31 candidate observations, compared
with none in the baseline. Copy removal is most visible in the valid zero-property legacy output;
rendering two timestamp properties dominates the usual output path, and the INT/TIMESTAMP
measurements regress rather than showing a universal timing gain. The artifact retains all three
sizes, unfavorable cases, and the earlier full-native-flush measurements.

Complete-job comparisons use 262,144 inputs, 65,536 groups, two warmups and five observations,
alternating stock/native order. BIGINT medians moved from stock/native 515.6/500.1 ms to
399.3/379.8 ms: stock drift tracks native drift, so the apparent 24% native improvement is not
causal evidence for this change. STRING medians were 582.6/591.1 ms before and 578.0/560.7 ms
after; distributions overlap, so this supports only a modest workload-specific improvement.
Native medians are close to stock in both workloads. All output checks passed; unrelated Delta
cleanup failures after the completed paired window classes do not invalidate those samples.

Reproduce with JDK 17 and `SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench
-Dtest=WindowOutputShapingBenchmark -Dsurefire.failIfNoSpecifiedTests=false
-Dshaping.warmup=20 -Dshaping.runs=31`, saving the baseline before changing production code.
`-Pbench` selects the default Flink 2.2.1 line; use `-Pflink-1.18,bench` for the released 1.18
compatibility line as a portable rerun command, not a reported measurement here. Complete commands and boundaries for the full flush and stock comparison
are recorded alongside the raw samples; use identical release libraries and fixtures in both runs.

Focused controls retained in the same artifact reran the two-property INT and TIMESTAMP(9)
cases at 16,384 rows in baseline/candidate/candidate/baseline order, with 100 warmups and 51
observations per fixture per run. All four runs used JDK 17's default tiered compilation and
identical frozen release native libraries. Each shape retains 204 observations total, with
102 baseline and 102 candidate samples; pooling does not treat separate runs as one independent
experiment.

| Focused shape | Baseline run A / B medians | Candidate run A / B medians | Pooled baseline → candidate median | Baseline / candidate IQR |
| --- | ---: | ---: | ---: | ---: |
| Narrowed INT | 9.157 / 9.224 ms | 9.125 / 9.632 ms | 9.198 → 9.438 ms | 8.891–9.736 / 9.126–9.766 ms |
| TIMESTAMP(9) | 9.342 / 9.213 ms | 9.134 / 8.870 ms | 9.263 → 9.032 ms | 9.123–9.439 / 8.779–9.573 ms |

The larger short-warmup regressions were not reproduced: pooled INT latency is still 2.6%
higher, while timestamp latency is 2.5% lower, with overlapping distributions and variation
between runs. This demonstrates sensitivity to warmup and process order rather than proving
that every case improves. The earlier unfavorable 20-warmup data remains intact; the
zero-property INT case was not retested by these focused controls.

Append `-Dshaping.shapes=INT,TIMESTAMP -Dshaping.rows=16384 -Dshaping.properties=2
-Dshaping.warmup=100 -Dshaping.runs=51` to the shaping command above to reproduce the focused
fixtures. No altered compiler flags were used in these reported controls.

The complete 30-case matrix was also repeated with 100 warmups and 31 observations per
fixture under the default tiered compiler. All 1,860 additional observations are retained in
the raw artifact as `final_shaping_before` and `final_shaping_after`. At 16,384 rows:

| Shape | No properties, before → after | Two properties, before → after |
| --- | --- | --- |
| BIGINT | 0.316 → 0.106 ms | 8.931 → 8.896 ms |
| Nullable BIGINT | 0.228 → 0.263 ms | 8.846 → 8.959 ms |
| STRING | 0.942 → 0.151 ms | 9.645 → 9.572 ms |
| Narrowed INT | 0.199 → 0.168 ms | 9.150 → 10.944 ms |
| TIMESTAMP(9) | 0.460 → 0.170 ms | 10.155 → 10.543 ms |

The zero-property INT regression did not persist, but two-property INT remains 19.6% slower
in this full-matrix repeat despite the smaller difference in focused ABBA controls. Nullable
BIGINT without properties also regressed 15.2%; timestamp with properties regressed 3.8%.
These results limit the optimization's performance claim: substantial copy savings are visible
in several zero-property cases, while property conversion dominates others and some measured
cases remain slower. Longer warmup alone does not resolve every regression. Reproduce this
matrix with the shaping command above and `-Dshaping.warmup=100 -Dshaping.runs=31`, leaving
shape, size, and property selectors unset.


The production candidate narrows transfer to zero-property legacy group windows. The following
measurements are **provisional and contended**, rather than final numerical performance proof.
An unrelated Fluss benchmark started at 07:29 in another worktree and overlapped the narrowed
shaping run and both legacy complete-job runs; two busy Java processes consumed approximately
5.5 GB while host swap was full. Raw observations remain visible, and the affected before/after
pair requires a quiet-host rerun. Structural sharing checks remain valid. It was compared with
`window-final-before.log` using the same 100-warmup, 31-observation matrix; the artifact retains
all 930 additional samples as `narrowed_shaping_after`. Largest-case medians are:

| Shape | No properties, before → narrowed | Two properties, before → narrowed |
| --- | --- | --- |
| BIGINT | 0.316 → 0.184 ms | 8.931 → 9.549 ms |
| Nullable BIGINT control | 0.228 → 0.160 ms | 8.846 → 9.139 ms |
| STRING | 0.942 → 0.150 ms | 9.645 → 9.988 ms |
| Narrowed INT | 0.199 → 0.151 ms | 9.150 → 9.107 ms |
| TIMESTAMP(9) | 0.460 → 0.139 ms | 10.155 → 9.931 ms |

Only zero-property results share buffers in this candidate. All five largest zero-property medians
are lower in this contended repeat; this does not establish a final performance result; property-bearing cases execute the baseline copy path and their remaining
variation does not measure a transfer optimization. The `BIGINT` and `NULLABLE_BIGINT` shaping
labels both use identical BIGINT keys and every-fifth-row-null results: they are repeated
execution-order controls, not distinct dtype paths. Earlier fixtures and measurements were
preserved rather than relabeled after measurement.

Complete zero-property jobs retain five stock/native observations per engine, after two warmups:
BIGINT stock/native medians change from 523.5/373.8 ms to 462.8/392.2 ms; STRING changes from
752.7/548.9 ms to 637.7/549.8 ms. The narrowed candidate is faster than stock in these runs
(1.18× and 1.16×), but native BIGINT regresses 4.9% against its previous native median and
STRING is essentially unchanged. Candidate BIGINT samples include 3.687 s and 2.620 s outliers;
stock also drifts between runs. These jobs do not establish an overall throughput improvement.
These contended measurements require a quiet-host rerun before claiming a numerical
zero-property boundary improvement. The earlier clean 100-warmup full matrix supports the
conservative scope decision, while structural checks establish correct buffer sharing.
Append `-Dhandoff.job.properties=0` to the complete-job command for the legacy group-window
query; the default remains the property-bearing TVF query. Raw job samples are retained as
`legacy_jobs_before` and `legacy_jobs_narrowed_after`.

The final recorded comparison is `accepted_final_before` / `accepted_final_after` in the raw
artifact (`window-quiet-before.log` / `window-quiet-after.log`). Despite the filenames, external
background workloads were active and explicitly accepted by the user. Both phases use the same
frozen baseline release Rust libraries, isolating the Java change from concurrent native changes.
The matrix has 100 warmups and 31 observations per fixture; complete legacy jobs have two
warmups and five observations per engine. Both runs completed successfully. This accepted pair
supersedes the earlier pending quiet-host rerun for the final comparison; earlier provisional
and rejected-candidate observations remain intact.

| Shape, 16,384 rows | No properties, before → after | Two-property copy control, before → after |
| --- | --- | --- |
| BIGINT | 0.328 → 0.150 ms | 9.577 → 9.185 ms |
| Nullable BIGINT duplicate control | 0.245 → 0.083 ms | 9.435 → 8.812 ms |
| STRING | 1.081 → 0.129 ms | 10.063 → 9.539 ms |
| Narrowed INT | 0.468 → 0.146 ms | 9.491 → 8.829 ms |
| TIMESTAMP(9) | 0.521 → 0.118 ms | 9.313 → 8.994 ms |

All zero-property candidate fixtures share result buffers in 31/31 observations; property-bearing
fixtures share none. Largest zero-property boundary medians are 2.20–8.38× faster in this recorded
pair, while the unchanged two-property controls move about 3–7%. This supports a measured
boundary improvement with visible background variance; it does not establish statistical
confidence or attribute every difference to copying.

Complete-job BIGINT stock/native medians are 382.7/371.9 ms before and 368.0/386.8 ms after;
STRING is 548.9/546.2 ms before and 547.7/536.5 ms after. Native BIGINT regresses 4.0% and is
5.1% slower than stock after; native STRING improves 1.8% and is 2.1% faster than stock after.
Native sample ranges are 357.6–393.4 → 361.9–392.2 ms for BIGINT and
535.5–557.0 → 533.9–545.8 ms for STRING. The boundary optimization therefore carries no
claim of a general complete-job throughput win. All 1,860 shaping observations and 20 job
observations from this pair are archived alongside historical evidence.
