# Fuse exact string consumers

Native Calc can compare selected `LPAD`, `RPAD`, and `OVERLAY` intermediates in Rust
without allocating the intermediate string. Other qualified consumers use the
Arrow bytes and optional Java UTF-16 cache together.

## Why a UTF-8 kernel alone is insufficient

Released Flink 1.18.1 and 2.2.1 count UTF-16 units in these functions. Truncating a
supplementary character can leave an isolated surrogate. It must remain distinct
from `?` until the consumer finishes, even though Java encodes both to the same
final UTF-8 bytes. For runtime input `s = U+1F600`:

```sql
SELECT LPAD(s, 1, 'x') = '?',
       REPLACE(LPAD(s, 1, 'x'), '?', 'Z')
FROM src;
-- false, '?' at final UTF-8 output
```

Encoding the padding result before evaluating equality or replacement produces
incorrect values. Original Java string caches can also contain UTF-16 units that
cannot be reconstructed from their encoded bytes.

## Technique and admission

A fixed set of borrowed UTF-8 spans, borrowed original Java UTF-16 spans and
individual surrogate units represents the intermediate. Equality compares intact
spans directly and visits UTF-16 units at split boundaries. Replacement writes
directly into the final Arrow offsets/value buffers, retaining adjacent surrogate
units until they can be joined or finally encoded with Java's `?` replacement.
ASCII mapping uses bulk byte loops. Scalar operands remain scalar instead of
being expanded into repeated Arrow arrays; non-ASCII producer literals prepare
their UTF-16 units once per batch.

Growing LPAD/RPAD equality first compares the requested UTF-16 length with the
literal's length. If they match, it compares the unchanged source and one period
of padding. Comparing the remaining literal slice with itself shifted by that
period proves all repetitions, including a partial final period. These are bulk
UTF-16 slice comparisons; the repeated padding and complete padded string are
never allocated. A split surrogate remains a unit throughout the comparison.

The certified growing LPAD/RPAD kernel under single-ASCII-character REPLACE maps one complete padding
period, then copies it in growing chunks directly into the final Arrow value
buffer. It handles the partial last period with UTF-16 spans and encodes an
isolated surrogate only after replacement finishes. Output capacity is checked
before copying. This avoids both the intermediate padded string and a separate
repeated padding buffer; the final STRING output still has to be written.

The borrowed-span kernels follow Comet's scalar-operand and buffer-reuse discipline, including its
`static_invoke/char_varchar_utils/read_side_padding.rs` implementation. It uses
released Flink's UTF-16 semantics. The same-Calc transport retains the encoded bytes
and Java code units for later operators.

The standalone overlay kernel can copy intact UTF-8 spans straight into the output
buffer. With two Java-cached operands it copies contiguous UTF-16 spans into a reused
buffer, then encodes that buffer once. ASCII units pack directly into bytes. Mixed or
split spans retain the original surrogate units until encoding. Prepared projection
benchmarks include the cost of storing both representations.
[Padding and overlay projection measurements](../benchmarks/scalar-functions-strings.md#padding-and-overlay-projections-with-utf-16-caches)

The planner recognizes these direct consumers:

| Consumer | Native conditions |
|---|---|
| `producer = literal`, `literal = producer` | Valid string literal, including NULL; Unicode and original Java caches supported |
| `producer <> literal`, `literal <> producer` | Same conditions as equality |
| `REPLACE(producer, old, new)` | A qualified native edit or case result feeds the UTF-16 replacement kernel |

Producer arguments must be direct inputs, valid literals, or lossless integer
widenings of direct inputs. LPAD/RPAD consumers admit truncation, unchanged
length and growth, including their NULL, negative-length and empty-padding
behavior under the cache/literal conditions above. `OVERLAY` admits three or four
arguments and preserves UTF-16 positions, integer narrowing, and the exception
for a wrapped suffix index outside the string.

Qualified padding comparisons against literals and `OVERLAY` comparisons with
provably safe literal indexes use the allocation-free kernel. Dynamic `OVERLAY`
indexes and the other qualified consumers can use the Arrow representation that
retains both bytes and optional UTF-16. Cached and byte-backed inputs select the
matching representation for each value.

An input reference as a comparison target can observe a Java cache populated
by the producer through an alias. Those comparisons and unsupported
surrounding expressions retain exact generated evaluation. Inter-operator STRING
identity remains a separate fallback tracked in
[issue #519](https://github.com/datafusion-contrib/StreamFusion/issues/519).
[Issue #521](https://github.com/datafusion-contrib/StreamFusion/issues/521) tracks
the remaining fusion and performance work, including cached Unicode REPLACE.

## Correctness evidence

- Release Rust core suite: 801 tests passed. Compiled Calc tests deliberately use
  an invalid evaluator ID for all nine admitted combinations, so an accidental
  JVM callback fails the test. They include cached and binary-only inputs, sliced
  batches, repeated batches, NULLs and empty batches. A separate invalid-ID test
  requires native growing comparisons, including positive matches and inequality.
  Growing replacement tests also require no callback and cover full/partial
  periods, mixed growing/truncating rows and split supplementary characters.
- Focused SQL/planner suites: 53 passed on Flink 2.2.1/JDK 17; 50 passed and three
  existing capability assumptions aborted on Flink 1.18.1/JDK 11. SQL tests require native work
  as well as matching values, including positive/negative equality filters.
- A local production-Calc differential check compared 61,617 admitted checks
  per release, including growing equality/replacement and positive output
  literals, against released Flink results. All matched. The 64 raw exception
  fixtures and unadmitted replacements remain outside this native-success
  count. Original Java caches include isolated units that can rejoin across the
  padding/source boundary.
- Unchanged released Flink `testLPad`, `testRPad` and `testOverlay` tests passed on
  both lines. These establish the reference semantics; the invalid-ID compiled
  tests independently establish the absence of callbacks.

## Measurements

Local Apple M4 Pro / 24 GiB release-build measurements, 2026-10-26. The final component
comparison uses JDK 17 for both Flink releases, one fresh JVM per engine/profile,
256 MiB initial and 512 MiB maximum Java heap, seven warmups and nine measured
trials of 65,536 rows. JDK 11 correctness is tested separately; the embedded JVM
benchmark could not start with JDK 11 on this host.

Stock Flink starts with prepared RowData; StreamFusion starts with equivalent
prepared Arrow batches. The Rust timer wraps the production compiled Calc,
allocation, intrinsic callbacks and result consumption. Input setup, initial JNI
entry/final export and perimeter transposes are excluded from this component
comparison. The previous path is the retained exact evaluator from the parent
implementation, with three runtime arguments and inline consumer literals.

Inputs have 16 distinct values, NULL every seventh row, dynamic truncation or
overlay positions, and nominal 264-byte strings. Actual values include a prefix
and cardinality suffix. `cold` means binary-only StringData; `cached` retains the
original Java string. Values are nanoseconds per row: **median [minimum-maximum]**.

| Flink | Expression / input, 1,024 rows per batch | Stock | Previous StreamFusion | Fused StreamFusion |
|---|---|---:|---:|---:|
| 2.2.1 | LPAD equality, ASCII cold | 184.07 [154.06-206.30] | 250.72 [216.72-439.16] | 41.75 [37.48-44.43] |
| 2.2.1 | LPAD equality, CJK cached | 81.19 [68.34-125.36] | 219.25 [192.18-244.17] | 22.27 [21.68-23.58] |
| 2.2.1 | OVERLAY equality, supplementary cold | 284.05 [266.84-295.46] | 357.80 [314.90-376.09] | 95.49 [93.83-98.33] |
| 2.2.1 | LPAD REPLACE, ASCII cached | 183.84 [161.58-220.70] | 338.37 [288.62-372.99] | 109.98 [107.59-112.24] |
| 2.2.1 | LPAD REPLACE, supplementary cold | 328.35 [322.74-338.47] | 403.14 [389.80-431.42] | 172.73 [166.25-177.13] |
| 2.2.1 | OVERLAY REPLACE, ASCII cold | 277.40 [265.39-296.11] | 367.71 [341.21-433.44] | 71.48 [70.13-75.49] |
| 1.18.1 | LPAD equality, ASCII cold | 178.27 [149.68-196.07] | 256.28 [236.91-265.75] | 41.29 [39.27-43.35] |
| 1.18.1 | LPAD equality, CJK cached | 93.84 [68.95-116.65] | 224.83 [179.74-243.18] | 22.61 [22.15-22.97] |
| 1.18.1 | OVERLAY equality, supplementary cold | 278.92 [261.62-291.11] | 337.17 [317.72-371.52] | 99.55 [97.56-103.89] |
| 1.18.1 | LPAD REPLACE, ASCII cached | 174.86 [144.72-199.21] | 315.44 [285.79-363.77] | 106.11 [104.87-112.91] |
| 1.18.1 | LPAD REPLACE, supplementary cold | 329.91 [316.52-338.27] | 413.96 [385.11-461.43] | 172.26 [168.03-176.93] |
| 1.18.1 | OVERLAY REPLACE, ASCII cold | 285.47 [264.56-298.60] | 372.95 [340.67-435.74] | 71.41 [69.62-74.56] |

These admitted profiles are 1.65-4.41x faster than stock Flink and 2.33-9.95x
faster than the previous StreamFusion evaluator. They are not a claim about all
STRING expressions or every batch size. Remaining unfavorable measurements:

| Flink | Profile | Stock | Previous StreamFusion | Current StreamFusion |
|---|---|---:|---:|---:|
| 2.2.1 | LPAD equality, cached ASCII, nominal 32 bytes, 16 rows | 39.78 [38.48-81.11] | 1339.03 [1234.66-2325.74] | 93.11 [89.47-96.46], native |
| 1.18.1 | Same small-batch profile | 42.06 [38.78-88.00] | 1251.60 [1206.23-1515.80] | 95.34 [92.73-105.59], native |
| 2.2.1 | OVERLAY REPLACE, cached CJK, 1,024 rows | 238.41 [232.06-259.62] | 373.51 [349.99-452.00] | 368.64 [345.99-434.81], retained evaluator |
| 1.18.1 | Same cached CJK profile | 227.76 [197.90-257.68] | 362.76 [351.95-445.05] | 378.37 [346.79-428.02], retained evaluator |

Small-batch compiled-Calc/array setup still exceeds stock's cached row loop, despite
the improvement over the already-admitted evaluator. Cached Unicode replacement
was profiled and remains on its existing evaluator because the proposed encoder
did not outperform stock. Neither case adds new SQL coverage or qualifies as an
acceleration over Flink. The fixed dispatch, encoding and boundary costs are part
of issue #521.

### Complete bounded job

The optional `StringFusionPipelineBenchmark` runs two million deterministic rows,
parallelism one, `LPAD(s,n,'X') = 'a?'`, lengths one through three, nominal 264-byte ASCII, NULL every seventh
row and a blackhole row sink. Both RowData-to-Arrow and Arrow-to-RowData remain in
the native INSERT plan and timer. Separate SQL tests establish output parity;
blackhole timings alone do not compare output values.

Flink 2.2.1/JDK 17, separate fresh JVMs with the same 2 GiB maximum heap, two
warmups and five measured runs; both engines explain INSERT before timing:

| Engine | Median seconds | Minimum-maximum seconds |
|---|---:|---:|
| Stock Flink | 0.690 | 0.687-0.691 |
| Previous StreamFusion | 0.751 | 0.747-0.764 |
| Fused StreamFusion | 0.560 | 0.559-0.591 |

The current path reduces elapsed time by 19% versus Flink and 25% versus previous
StreamFusion in this bounded job. These short local jobs include startup and
planning; they do not establish sustained connector throughput. Earlier
interleaved runs with a larger 8 GiB heap measured 0.509 seconds for Flink versus
0.611 seconds for the candidate. That unfavorable diagnostic is retained: heap,
warmup and execution order materially affect short-job results. The controlled
three-way comparison above uses identical resources and fresh JVMs throughout.

### Growing padding comparisons

Growth comparisons use the same component boundaries, released Flink versions,
JDK 17, hardware and heap sizes, with eight warmups and nine measured trials of
131,072 rows per engine/profile. The previous control links the same benchmark
driver against the preceding production library, including its real certified
dispatch and JVM evaluation for growing padding. All inputs match, including
dummy length values on NULL source rows. The candidate uses an invalid evaluator
ID and checks the result against the generated evaluator before timing.

The 28 profiles cover both releases, LPAD equality/RPAD equality and inequality,
ASCII/CJK/supplementary sources and padding, original Java caches/binary-only
inputs, 16/1,024/16,384-row batches, and mixed/all-matching distributions. Mixed
inputs have cardinality 16 and vary the requested length by zero to two units;
all-matching inputs have cardinality one and a constant length. Both retain NULL
every seventh row. CJK growth uses U+4E2D padding and supplementary growth uses
U+1F600 padding; ASCII uses `X`.

An initial per-period scan took 188 ns/row for cached 32-byte ASCII with all
non-NULL rows matching and 64 added units. Comparing shifted literal slices
instead reduces that profile to 24 ns/row. This avoids rescanning each padding
repetition, rather than relying on early mismatches for its benefit.

Selected final results, median [minimum-maximum] ns/row; 1,024-row batches unless
specified. The [complete 28-profile summary](../benchmarks/native-string-padding-growth-20261026.csv)
retains both releases and every measured range.

| Flink | Growth profile | Stock | Previous StreamFusion | Native |
|---|---|---:|---:|---:|
| 2.2.1 | LPAD, ASCII 32 bytes, cold, mixed, +64 units | 167.24 [151.50-170.53] | 215.64 [210.93-218.02] | 18.75 [18.25-19.10] |
| 2.2.1 | LPAD, ASCII 32 bytes, cached, all matching, +64 | 142.58 [126.28-176.71] | 219.00 [211.75-234.74] | 23.76 [23.49-25.96] |
| 2.2.1 | RPAD, ASCII 264 bytes, cold, all matching, +16 | 170.07 [167.77-183.90] | 227.41 [223.26-237.69] | 128.75 [126.00-130.11] |
| 2.2.1 | LPAD, CJK 264 bytes, cached, all matching, +16 | 112.61 [107.01-127.31] | 216.51 [212.15-221.08] | 25.05 [24.04-27.62] |
| 2.2.1 | RPAD inequality, supplementary 264 bytes, cached, all matching, +4 | 107.88 [95.75-167.97] | 202.97 [197.94-235.93] | 24.29 [24.15-24.82] |
| 2.2.1 | LPAD, ASCII 32 bytes, cached, all matching, +64, **16 rows** | 113.77 [104.05-122.51] | 1363.80 [1357.92-1380.78] | 96.73 [94.13-97.97] |
| 1.18.1 | LPAD, CJK 264 bytes, cold, all matching, +16 | 243.83 [239.44-268.87] | 303.06 [301.95-314.62] | 216.26 [206.84-228.87] |
| 1.18.1 | RPAD inequality, supplementary 264 bytes, cached, all matching, +4 | 93.44 [85.23-111.73] | 227.52 [203.69-233.12] | 24.91 [24.53-26.53] |

All 28 sampled profiles improve over stock (1.13-9.01x) and previous StreamFusion
(1.40-14.74x). The smaller margins on binary-only Unicode reflect UTF-16 counting
and comparison work; the 16-row profile still pays fixed compiled-Calc setup.
These measurements do not establish a win for every growth length or batch size.
The earlier unfavorable truncation/REPLACE diagnostics remain applicable to their
separate contexts.

The complete bounded growth job uses the same two million rows, parallelism one,
both transposes and blackhole row sink, with requested lengths 280-282 and an
LPAD comparison against the 280-unit literal for source value zero. Independent
fresh JVMs use the same 2 GiB maximum heap, two warmups and five measured trials:

| Engine | Median seconds | Minimum-maximum seconds |
|---|---:|---:|
| Stock Flink | 0.825 | 0.815-0.860 |
| Previous StreamFusion | 0.925 | 0.909-1.072 |
| Native growth comparison | 0.552 | 0.543-0.560 |

Elapsed time decreases 33% versus stock and 40% versus previous in this bounded
job. The same startup/planning and blackhole-output limitations described above
apply. Runtime comparison targets and inter-operator STRING
transport remain unmodified and tracked in
[issue #521](https://github.com/datafusion-contrib/StreamFusion/issues/521) and
[issue #519](https://github.com/datafusion-contrib/StreamFusion/issues/519).

### Growing padding under REPLACE

The next comparison measures `REPLACE(LPAD/RPAD(s,n,p), '?', 'Z')` with growing
lengths. It uses the same component boundaries and Apple M4 Pro/24 GiB hardware,
JDK 17 for each released Flink line, fresh JVMs, 256/512 MiB initial/maximum heap,
eight warmups and nine measured trials of 131,072 rows per profile/engine. The
previous control links the same benchmark driver against production revision
`99a13b5b`, including its certified dispatch and retained JVM growth evaluator.
The candidate uses an invalid evaluator ID and compares complete output arrays
against the generated evaluator before timing.

Sixteen profiles cover both releases, LPAD/RPAD, nominal 32/264-byte inputs,
ASCII/CJK/supplementary sources and padding, cached ASCII and binary-only inputs,
16/1,024/16,384-row batches, cardinality 16 and NULL every seventh row. Requested
growth varies by zero to two additional units. Padding is `X?`, U+4E2D followed
by `?`, or U+1F600 followed by `?`, so replacement visits the padding as well as
the source. Original non-ASCII Java caches remain outside admission.

Selected medians [minimum-maximum] ns/row, 1,024-row batches unless specified.
The [complete 16-profile summary](../benchmarks/native-padding-replace-growth-20261026.csv)
retains all measured ranges.

| Flink | Growth replacement profile | Stock | Previous StreamFusion | Native |
|---|---|---:|---:|---:|
| 2.2.1 | LPAD, ASCII 32 bytes, cold, +64 units | 203.69 [189.49-206.67] | 265.21 [258.80-286.29] | 50.16 [49.46-52.94] |
| 2.2.1 | RPAD, ASCII 264 bytes, cold, +16 units | 279.45 [276.17-288.93] | 355.65 [341.68-366.44] | 53.96 [51.04-54.87] |
| 2.2.1 | RPAD, ASCII 264 bytes, cached, +16 units | 237.43 [233.85-241.93] | 353.13 [348.28-378.58] | 109.54 [108.91-112.72] |
| 2.2.1 | LPAD, CJK 264 bytes, cold, +16 units | 377.32 [371.91-388.84] | 459.91 [451.81-470.93] | 217.87 [215.02-223.24] |
| 2.2.1 | RPAD, supplementary 264 bytes, cold, +4 units | 388.50 [380.83-397.39] | 465.12 [462.85-491.24] | 182.34 [179.99-185.31] |
| 2.2.1 | LPAD, ASCII 32 bytes, cached, +64 units, **16 rows** | 152.67 [145.05-157.60] | 1548.11 [1527.01-1623.21] | 143.55 [137.57-149.78] |
| 1.18.1 | LPAD, CJK 264 bytes, cold, +16 | 360.58 [358.83-363.48] | 446.01 [439.97-461.95] | 217.78 [213.57-224.67] |
| 1.18.1 | RPAD, supplementary 264 bytes, cold, +4 | 379.23 [375.21-392.51] | 468.21 [452.55-479.91] | 184.76 [181.90-185.70] |

Median ratios are 1.06-5.66x versus stock and 2.05-10.86x versus the previous
production path. The 16-row Flink 2.2 profile improves only 6%, with overlapping
sample ranges, so it does not demonstrate a substantial small-batch gain. The
cached ASCII source still pays UTF-16-to-byte mapping, and binary-only Unicode
still pays UTF-16 counting. No universal speedup is claimed.

A separate Flink 2.2.1/JDK 17 bounded job uses two million rows, parallelism one,
lengths 280-282 and `REPLACE(LPAD(s,n,'X?'), '?', 'Z')`. Both RowData/Arrow
transposes and the blackhole row sink remain in the plan and timer. Its sink
type is derived from the query, so the final STRING is written through the same
row boundary rather than reduced to a BOOLEAN or checksum inside the query.
Fresh JVMs use identical 2 GiB maximum heaps, two warmups and five measured
trials:

| Engine | Median seconds | Minimum-maximum seconds |
|---|---:|---:|
| Stock Flink | 1.370 | 1.363-1.396 |
| Previous StreamFusion | 1.490 | 1.476-1.530 |
| Native growth replacement | 0.792 | 0.758-0.836 |

Elapsed time decreases 42% versus stock and 47% versus previous for this short
bounded workload, including startup/planning. This does not measure sustained
connector throughput. Separate parity tests verify values; blackhole timings
alone do not. Dynamic replacement operands, non-ASCII original caches and the
other contexts linked above remain on their existing paths.

A regression check of existing truncating LPAD/OVERLAY replacement profiles found
that outlining the shared writer introduced several percent overhead. The final
writer is inlined. Six repeated control profiles across both releases have
median changes from 4.1% lower to 2.7% higher than the preceding production
revision, with overlapping ranges for each pair. They remain faster than stock;
no improvement is claimed for these unchanged contexts. The
[complete control summary](../benchmarks/native-padding-replace-controls-20261026.csv)
retains their measurements. For Flink 2.2.1, ns/row:

| Existing profile, 1,024 rows | Stock | Previous StreamFusion | Current |
|---|---:|---:|---:|
| LPAD, ASCII, cold | 218.44 [214.16-233.85] | 48.12 [45.92-50.83] | 48.40 [44.52-49.81] |
| LPAD, ASCII, cached | 161.73 [154.25-179.81] | 101.96 [99.35-142.76] | 98.05 [94.63-101.43] |
| OVERLAY, supplementary, cold | 385.86 [383.32-393.65] | 174.10 [170.17-182.57] | 178.74 [174.55-184.61] |

## Reproduce

From the repository root, using JDK 17 and the pinned Rust toolchain:

```sh
mvn -pl streamfusion-runtime -Pbench -Dnative.build.skip=true \
  -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/string-fusion.classpath
(cd native && cargo build --release --no-default-features -p streamfusion \
  --lib --example string_fusion)
export SF_NATIVE_BENCH_CLASSPATH="$PWD/streamfusion-runtime/target/test-classes:$PWD/streamfusion-runtime/target/classes:$(cat streamfusion-runtime/target/string-fusion.classpath)"
export SF_NATIVE_LIB_DIR="$PWD/native/target/release"
native/target/release/examples/string_fusion 20 ascii 264 false 1024 64 7 9 flink
native/target/release/examples/string_fusion 20 ascii 264 false 1024 64 7 9 previous
native/target/release/examples/string_fusion 20 ascii 264 false 1024 64 7 9 native
# Growing, all-matching cached inputs; one process per engine:
native/target/release/examples/string_fusion 20 ascii 32 true 1024 128 8 9 flink 64 true
native/target/release/examples/string_fusion 20 ascii 32 true 1024 128 8 9 native 64 true
# Growing replacement, binary-only supplementary input and padding:
native/target/release/examples/string_fusion 27 emoji 264 false 1024 128 8 9 flink 4
native/target/release/examples/string_fusion 27 emoji 264 false 1024 128 8 9 native 4
```

Each command starts a fresh JVM. Operation codes 20/21/22 are LPAD/RPAD/OVERLAY
equality; 23/24/25 are inequality; 26/27/28 are single-ASCII-character REPLACE.
The remaining arguments select corpus (`ascii`, `cjk`, `emoji`), nominal width,
original Java cache, batch rows, loops, warmups, measured trials and engine.
CSV records contain operation, corpus, width, cache, batch rows, total timed rows,
trial, warmup flag, actual engine, total nanoseconds, consumed checksum, growth
units and all-matching flag. Two optional final arguments select padding growth
units (default zero, restricted to LPAD/RPAD consumers) and an all-matching
distribution (default false). All-matching inputs have cardinality one and a
constant requested length; NULL every seventh row remains unchanged.
Growing REPLACE uses `X?`, U+4E2D followed by `?`, or U+1F600 followed by `?`
padding so that replacement occurs in the padding as well as the source.
`retained` explicitly means that the candidate called its exact evaluator.
`previous` measures the exact generated evaluator control. A separate comparison
binary linked against the preceding production revision uses `baseline` to time
that revision's certified expression dispatch, including its retained evaluator;
running `baseline` against the current library is not a previous-version result.

Run the optional full job separately with `SF_BENCHMARK=true`, the `bench` Maven
profile and `-Dtest=StringFusionPipelineBenchmark`. The `fusion.pipeline.rows`,
`fusion.pipeline.warmups`, `fusion.pipeline.runs`, `fusion.pipeline.expression`,
`fusion.pipeline.minLength` (default one) and
`fusion.pipeline.engines=stock|native|both` properties select the workload.
Use identical heap and JVM settings for before/after comparisons, and a separate
worktree at the parent revision for the previous production encoder.
For the complete growing replacement job, select
`-Dfusion.pipeline.minLength=280` and
`-Dfusion.pipeline.expression="REPLACE(LPAD(s,n,'X?'), '?', 'Z')"`.
The blackhole sink derives its column type from the selected expression.
