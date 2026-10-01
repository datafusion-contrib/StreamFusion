# Scalar function allocation and text scans

These techniques describe the current function kernels inside native Calc. The
[scalar function benchmark page](../benchmarks/scalar-functions.md) reports Flink and native
complete-job results for the same workloads, including both row/Arrow transposes. Those
measurements do not isolate kernel cost, and native admission does not guarantee a speedup
for each standalone query.

The optional Criterion `scalar_functions` benchmark invokes production UDFs on 4,096-row Arrow
arrays, including scalar adaptation, NULL handling, and result allocation. Input creation,
planner coercion, JNI, and transposes are excluded from that diagnostic:

```sh
cargo bench --manifest-path native/Cargo.toml -p streamfusion --features mimalloc --bench scalar_functions
```

Run one benchmark at a time. Kernel diagnostics and complete SQL jobs measure different work.

## FROM_UNIXTIME

Literal numeric patterns in fixed-offset zones compile to Rust formatting tokens once per
expression. This removes the scalar JVM callback and its Arrow export/import from the
formatting path. Integer calendar arithmetic preserves Java's historical calendar cutover
and overflow range; unsupported formats and transition zones keep the Flink evaluator.

For the same two-million-row queries with both transposes, two warmups and five measured
runs, switching from the previous JVM bridge to native formatting reduced the default-format
median from 1.696912s to 1.003518s (1.691x), and `yyyyMMddHHmm` from 1.543752s to 0.958994s
(1.610x). The identity control remained 0.655586s versus 0.651136s. These sequential native-only
comparisons are separate from the interleaved Flink/native measurements on the
[scalar benchmark page](../benchmarks/scalar-functions.md).

## LOCATE

The two-argument form reverses operands into DataFusion strpos. The three-argument Rust kernel follows DataFusion's batch ASCII detection, one reusable memmem::Finder for literal needles, and memmem searches for column needles. Constant needles/starts stay scalar, following Comet's contains pattern. Shared positioning code preserves Flink's empty-needle and signed-start rules; no JVM callback is added.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#locate).

## GREATEST

Integer and matching-scale Decimal extrema use primitive Arrow comparison loops, folding constants once and combining validity masks. ASCII-provable string/Boolean extrema reuse DataFusion with Flink's strict NULL mask. This avoids intermediate Boolean selection arrays and expanded scalar arrays.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#greatest).

## LEAST

Reuses the shared extremum kernel with minimum comparison, including primitive Arrow loops, folded constants, and strict NULL masking. Matching decimal scale is preserved on the output.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#least).

## TRANSLATE

ASCII mappings use direct lookup; non-ASCII codepoints use the project's ahash map. Consecutive equal alphabets reuse the mapping. Duplicate source positions retain their first mapping, including deletion mappings.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#translate).

## BTRIM

Default and literal-set trimming both reuse DataFusion's btrim kernel. Keeping one implementation avoids a duplicate space-only scan while preserving the verified literal-set semantics.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#btrim).

## ELT

A scalar index returns the selected array with its buffers/NULL mask intact. Longer dynamic outputs are sized before writing; short strings keep a single writing pass to avoid a selection vector larger than their payload.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#elt).

## OVERLAY

Copies intact UTF-8 prefix/replacement/suffix slices when UTF-16 cuts align with codepoints; ASCII needs no UTF-16 conversion. Cuts inside surrogate pairs retain full UTF-16 reconstruction, recombined pairs, and Java's encoding of lone surrogates.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#overlay).

## ENCODE

The verified UTF-8 form reinterprets the same Arrow offsets, values, and validity as Binary, so the kernel copies no payload. The charset stays scalar through invocation, avoiding a full-length constant array; single-byte charsets reuse a scratch buffer.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#encode).

## DECODE

A successful safe Arrow Utf8 construction validates the BinaryArray once and reuses its buffers. Malformed byte sequences use the existing JDK-compatible replacement decoder and one reusable output buffer. This avoids per-row output allocation on valid UTF-8 without assuming that arbitrary bytes are valid.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#decode).

## JSON_UNQUOTE

Combines first-token JSON validation and unescaping in one scan, writes UTF-8 directly, and retains only a pending high surrogate instead of a whole intermediate UTF-16 vector. Flink's treatment of trailing text remains covered by SQL differential tests.

This kernel serves direct projections. JSON_UNQUOTE and its scalar consumers fuse into a Flink
expression when intermediate UTF-16 identity is observable; strings passed to another operator
fall back under the [Calc/filter restrictions](../operators/calc-filter.md#json_value).

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#json_unquote).

## SPLIT

Single-byte separators use Rust's character searcher (memchr) instead of the general substring searcher. One Arrow List builder accumulates each batch; complete-job results also include materializing the array output back into Flink rows.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#split).

## SUBSTRING

ASCII uses byte offsets. Negative positions in Unicode strings traverse backward only to the requested start, replacing a full character count followed by a second forward scan. The selected UTF-8 span is borrowed and copied into the Arrow builder.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#substring).

## LPAD

Following Comet's padding structure, the kernel writes intact UTF-8 prefixes and repeated pattern spans directly into the Arrow builder. A small prefix object tracks UTF-16 units and a cut surrogate, avoiding three whole-row UTF-16 buffers. Unpaired high surrogates still become the JDK '?' replacement. The ASCII path computes prefix positions from byte offsets.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#lpad).

## RPAD

Reuses the same prefix and padding-span machinery on the right, including Flink's UTF-16 cut behavior. It avoids whole-row transcoding while retaining exact results for cuts through supplementary characters.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#rpad).

## QUARTER

Fuses timestamp unit conversion, Flink's truncating day calculation, and integer Julian-calendar extraction into one primitive Arrow mapping. Besides eliminating intermediate arrays, this preserves Flink's expanded-year integer arithmetic outside chrono's range.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#quarter).

## Integer/string casts

CAST and TRY_CAST between character strings and signed integers now build Arrow arrays directly.
This removes the previous host cast's per-batch JNI callback and Arrow export/import. Parsing
borrows each string, validates Flink's decimal-text grammar and appends the requested integer
width without a temporary array. Formatting widens primitive inputs to BIGINT, then reuses one
small decimal scratch string per batch before writing the final Arrow string buffer. Error mode
and output length are part of the immutable expression, following Comet's cast dispatch pattern.
The [coverage page](../operators/calc-filter.md#integerstring-casts) records Flink-specific semantics.

On September 16, 2026, Apple M1 Max, JDK 17, Flink 2.2.1, release Rust with mimalloc and one
codegen unit: 2,000,000 rows, parallelism 1, two warmups and five interleaved trials per engine,
separate JVMs before/after. The before code is `baf2aa63` plus the same benchmark cases. All plans
assert NativeCalc and both row/Arrow transposes; input remains a row source and output a row
blackhole sink. Median complete-job seconds:

| Expression | Flink before | Flink after | Native host cast before | Native kernel after | Native time reduction |
|---|---:|---:|---:|---:|---:|
| `CAST(s AS INT)` | 0.581 | 0.586 | 1.014 | 0.835 | 17.7% |
| `CAST(n AS STRING)` | 0.740 | 0.751 | 1.222 | 1.142 | 6.6% |

String-source native identity controls were 1.035s before and 1.049s after; integer-source controls
were 0.789s and 0.819s. Both cast jobs improve relative to the previous native implementation,
but remain slower than stock Flink (0.702x and 0.657x after). These figures do not isolate kernel
cost or establish an overall workload speedup. The string input cycles through signed extrema,
zero, ordinary digits and a space-padded signed decimal; its fixed values ignore `scalar.bytes`.

```sh
SF_BENCHMARK=true mvn -B -ntp -pl streamfusion-runtime -am test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=STRING_TO_INT,INT_TO_STRING \
  -Dscalar.output=target/integer-string-casts.csv
```


## Integer ARRAY_DISTINCT

Integer arrays retain DataFusion's ordered-membership and batch-gather structure,
but use primitive integer membership rather than encoding each element into a
row. Arrays with at most eight elements use stack storage and a 64-bit fingerprint
that skips equality searches when the low six bits have not appeared. A
fingerprint collision always performs full integer equality, including signed
extrema. Larger arrays reuse a hash set across rows. NULL membership is tracked
separately, preserving its first occurrence.

One index vector gathers selected children for the entire batch; an already
unique visible batch shares the original buffers. List offsets and validity
preserve sliced input semantics. This changes no JNI or ownership boundary.
Profiling the initial generic kernel identified encoded-row/hash membership as
useful optimization targets, with the sampling limits documented alongside the
[whole-job measurements](../benchmarks/scalar-functions.md#integer-array_distinct-2026-09-28).

On Intel Core i7-12650H, Flink 2.2.1, release/mimalloc, 200,000 BIGINT arrays of
64 elements improved from the prior fallback's 1.049s to 0.310s; matched stock
Flink took 1.047s. The 256-element case took 0.924s versus stock 13.388s. With both
row/Arrow transposes retained, twenty million eight-element arrays were effectively
tied in the repeat (5.413s native, 5.430s stock). Small-array gains are not stable
across runs; the benchmark page retains the unfavorable intermediate results.

## Binary casts and selection

Binary casts and INT-indexed binary ELT operate on Arrow byte arrays. Casts truncate bytes,
zero-pad BINARY, and retain unpadded VARBINARY/legacy results. ELT copies only the selected
value into the output, preserving selected NULLs and out-of-range NULLs. Fixed and variable
binary inputs are borrowed directly during selection, avoiding a full fixed-to-variable
conversion of every input column. The existing result cast still enforces the declared width. Fixed results carry
their declared Arrow width, so the planner need not generate a whole Java Calc merely to
materialize that width. Flink evaluation remains responsible for expressions whose operands
can fail in ways requiring its rowwise evaluation order; fixed-binary TO_BASE64 retains its
verified generated path.

The previous cast decoded Arrow UTF-8 into a Java string and encoded it back into bytes.
A 20-second async-profiler CPU recording attributed 45% of samples to stacks containing
Flink's UTF-8 encoder, including both source conversion and callback work. The native cast
removes the callback conversion while retaining the source transpose. Released Flink's
StringToBinaryCastRule/BinaryToBinaryCastRule and EltFunction define the byte and index contracts.
The batch ownership model follows Comet's import/evaluate/export bridge; no ownership protocol changes.

Release+mimalloc, Core i7-12650H/Linux, JDK17/Flink2.2.1, 2 GiB heap, parallelism 1,
2M runtime rows, 264-byte text, no NULL injection, 1,024-row batches, two warmups and five
alternating trials. Row source, row sink and both transposes remain measured. Seconds:

| Query | Previous native median | Optimized native median (range) | Flink median (range) |
|---|---:|---:|---:|
| String to BINARY(16) | 1.748 | 0.969 (0.953–0.975) | 0.881 (0.857–0.889) |
| BINARY(16) ELT | 0.565 | 0.438 (0.431–0.440) | 0.312 (0.312–0.321) |

Native elapsed time improves 44.6% and 22.4%, respectively. Both remain slower than Flink;
these results do not establish merge readiness under the performance requirement. The same-run
identity controls are 1.012s native / 0.875s Flink for text and 0.407s / 0.314s for fixed binary.
The previous-run Flink medians were 0.906s and 0.330s; control variation does not explain the
large cast improvement but limits conclusions about small differences.

Reproduce with `ScalarFunctionBenchmark#individualFunctions`, `-Pbench`,
`-Dscalar.functions=TRY_STRING_TO_FIXED_BINARY,ELT_FIXED_BINARY`, `-Dscalar.rows=2000000`,
`-Dscalar.nullEvery=0`, `-Dscalar.warmup=2`, `-Dscalar.runs=5`,
`-Dsf.extraJvmArgs=-Xmx2g`, and `SF_BENCHMARK=true`.

A later ELT CPU profile (20 seconds per engine) attributed 2.4% of native samples to
fixed-to-variable binary conversion of input columns and 1.7% to the result conversion.
Removing the input conversions reduces the native ELT median from 0.420s (0.412–0.424)
to 0.399s (0.393–0.403), a 4.9% reduction with non-overlapping ranges. The matched Flink
control stays at 0.308s (previous 0.307–0.320, candidate 0.301–0.316). Both runs include the
[generated binary exit](zero-copy-exit-transpose.md#binary-output-measurements), so this
comparison isolates the additional input-copy removal. Native ELT still trails Flink by
about 30%; this is a partial optimization, not evidence that the PR is ready to merge.
The unchanged cast measures 0.926s native / 0.856s Flink. Identity controls remain unfavorable:
text 1.008s / 0.883s and fixed binary 0.374s / 0.308s. Configuration is unchanged from the
2M-row comparison above. [Raw trials](../benchmarks/scalar-binary-exit-2026-09-28.csv) include
both the exit-only and borrowed-input runs. Native tests cover mixed fixed/variable inputs,
slices, empty values, NULLs and invalid indexes.


### Binary keys composed with grouped counts

A September 28 follow-up measures `COUNT(*)` grouped by
`TRY_CAST(s AS BINARY(16))` or `ELT(n,b,X'00112233445566778899AABBCCDDEEFF')`.
It retains the source inputs from the standalone benchmark, yielding two text-cast
keys and three ELT groups (including the NULL group from invalid indexes). These
are low-cardinality composition diagnostics, not high-cardinality grouping results.
There is no additional NULL injection. Both engines use identical mini-batch settings
(1,024 rows, maximum latency five seconds). The native plan must include NativeCalc,
NativeColumnarGroupAggregate, and both row transposes. The release+mimalloc, Flink
2.2.1/JDK 17, parallelism one, 2 GiB heap, two warmups and five alternating trials
match the standalone configuration. Complete-job medians and trial ranges in seconds:

| Query | Rows | Native median (range) | Flink median (range) |
| --- | ---: | ---: | ---: |
| String to BINARY(16), grouped COUNT | 2M | 0.985 (0.960–0.986) | 1.217 (1.198–1.231) |
| BINARY(16) ELT, grouped COUNT | 2M | 0.431 (0.427–0.447) | 0.666 (0.647–0.693) |
| Text identity | 2M | 0.988 (0.985–1.014) | 0.872 (0.853–1.011) |
| Fixed binary identity | 2M | 0.388 (0.384–0.400) | 0.327 (0.307–0.330) |
| String to BINARY(16), grouped COUNT | 5M | 2.298 (2.281–2.314) | 2.962 (2.908–2.983) |
| BINARY(16) ELT, grouped COUNT | 5M | 1.180 (1.176–1.183) | 1.597 (1.574–1.689) |
| Text identity | 5M | 2.403 (2.357–2.419) | 2.097 (2.074–2.143) |
| Fixed binary identity | 5M | 0.820 (0.818–0.837) | 0.641 (0.641–1.021) |

The grouped queries take 19–35% less native elapsed time at 2M rows and 22–26%
less at 5M, with disjoint observed ranges. This benefit belongs to the complete
native expression-plus-aggregate pipeline. It does not erase the slower standalone
binary projections or identity controls, nor measure a previous-StreamFusion
composition baseline. The standalone performance requirement remains unresolved.

Grouped parity tests compare materialized results on NULLs, invalid ELT indexes,
non-text bytes, zero padding, UTF-8 truncation and multiple input batches. Both
cases pass on Flink 2.2; the cast case passes on 1.18, where ELT is unavailable and
its case is explicitly skipped. Existing tests retain rowwise failure-order coverage.

[Raw trials](../benchmarks/binary-composition-2026-09-28.csv) include both engines
and identity controls. Reproduce with the same `ScalarFunctionBenchmark#individualFunctions`
release command above, setting
`-Dscalar.functions=TRY_FIXED_BINARY_GROUPED_COUNT,ELT_FIXED_BINARY_GROUPED_COUNT`
and `-Dscalar.rows=2000000` or `5000000`. The Nexmark harness is unchanged.

## Direct fixed-binary output buffers

Fixed-result casts and ELT now produce the declared Arrow `FixedSizeBinary` array directly.
Previously the UDF built variable binary and Calc converted it again to fixed binary, allocating
an offsets buffer and copying the result payload twice. Fixed input casts also borrow their input
bytes rather than first materializing a variable-binary array. Casts truncate bytes into the final
buffer and extend zero padding directly there. A fixed cast with unchanged width retains the
original sliced array and validity bitmap. Variable/legacy results keep their existing representation.

The `binary_expressions` Criterion suite runs the production compiled Calc path and asserts result
bytes and fixed widths before measurement. The following comparison uses 1,024 sliced input rows,
width 16, non-null inputs, and the same system-allocator instrumentation in both runs. ELT includes
NULL results from invalid/null indices. Fixture construction and allocation-report traversal are
outside measurement; output disposal is inside. Standard Criterion sampling uses 100 samples,
three-second warmup, and five-second measurement per case.

| Operation | Previous warm Calc | Direct-buffer warm Calc | Previous allocation requests | Direct allocation requests |
| --- | ---: | ---: | ---: | ---: |
| String to fixed binary | 10.048 µs | 4.644 µs | 38,799 B | 17,843 B |
| Fixed binary width change | 13.064 µs | 3.666 µs | 107,571 B | 17,659 B |
| Fixed binary ELT | 10.654 µs | 5.486 µs | 38,361 B | 18,357 B |

Criterion reports time reductions of 53.8%, 72.0%, and 48.0%, respectively, with disjoint
confidence intervals. The result payload remains 16,384 bytes (ELT also has a 128-byte validity
bitmap); these are reductions in intermediate allocation and materialization, not elimination of
the necessary output buffer. Allocation requests do not measure peak memory or copied bytes.
All 72 fixtures across 16/1,024/16,384 rows, widths 1/16/256, input representations and null profiles
pass release smoke checks. These native timings do not establish an end-to-end Flink win; the
standalone and composition measurements above remain the admission evidence until rerun.

Reproduce the native comparison with
`python3 bin/bench-native.py --bench binary_expressions --filter '(string_cast|fixed_cast|elt)/1024/width=16/nulls=false'`
and `--save-baseline before` / `--baseline before`, applying the same production diff between runs.

## ARRAY_DISTINCT defers gather indices until a value is removed

The typed deduplication kernels share the input Arrow array when every visible element is already
distinct. They now also avoid constructing gather indices and normalized offsets in that case.
Until the first duplicate or nonempty NULL-container span, retained positions form a contiguous
visible prefix. Only at that first removal does the kernel materialize that prefix, then append
subsequent selected positions. Hidden child values belonging to NULL containers are excluded,
including after a sliced, unchanged prefix. Element order, field metadata, nullability, and the
existing Arrow gather path for changed arrays are preserved.

The `collection_expressions` release suite covers Boolean, integer and UTF-8 arrays, unique and
repeated values, sliced/null input, short/large lists, and narrow/wide strings. All 84 fixtures
pass after the change. The Boolean/string extension is under review; nine runtime SQL checks pass against released Flink 2.2.1; whole-job Flink evidence remains pending.

On Rust 1.94.0/Arrow 58.3.0, Linux/Core i7-12650H, sequential Criterion runs of warm compiled
Calc with 1,024 non-null arrays of 64 unique values report:

| Input | Previous mean | Lazy selection mean | Previous allocation requests | Lazy allocation requests |
| --- | ---: | ---: | ---: | ---: |
| INT | 347.785 µs | 340.647 µs | 268,819 B | 2,575 B |
| STRING, 264-byte suffix plus UTF-8/key prefix | 1.878 ms | 1.804 ms | 271,843 B | 5,599 B |

Criterion reports 2.1% and 3.9% lower time with disjoint 95% mean confidence intervals.
An earlier revision measured 3.3% and 12.2%; retain it in the CSV rather than selecting the
stronger preliminary result. The final revision avoids calling the prefix-building helper
again once selection has started. Hash-set seeding and timing variability remain relevant.
Both versions already share all output payload buffers for these unchanged arrays; this removes
index/offset allocation and filling, rather than a payload copy. Fixture construction and probe
traversal remain outside measurement. Counting uses the same benchmark system allocator in both
runs, with 100 samples, three-second warmup and five-second measurement per case.
[Mean confidence intervals and allocation probes](../benchmarks/recovered-historical-diagnostics-2026-10-01.txt)
are partially recoverable from the task log. The original complete CSV was lost during an environment reset. Reproduce using `--bench collection_expressions`,
`--filter '(integer|string)/1024/width=64/domain=64/bytes=(0|264)/nulls=false'`, and
`--save-baseline before` / `--baseline before`. Native timings do not establish an end-to-end
Flink speedup. Runtime SQL checks now pass; whole-job comparisons remain pending.

The environment reset removed temporary benchmark artifacts. Historical links above now point to surviving task-log excerpts; complete raw CSVs and Criterion samples must be regenerated.

## Boolean/string collection whole-job profiles

`DynamicCollectionBenchmark` accepts `collection.type=BOOLEAN|STRING|INT|BIGINT`; the
existing `collection.int` option retains its default mapping. ARRAY_DISTINCT measurements
use `collection.expression=ARRAY_DISTINCT(arr)` and `collection.map=false`, retaining the
runtime row source, blackhole sink, JNI and both transposes. `collection.width` and
`collection.domain` vary list width and duplicates; strings include Unicode and NUL with
`collection.bytes` suffix characters. The source reuses an immutable value catalogue equally
for both engines and constructs each row's array outside the expression evaluation.
`collection.nullEvery` and `collection.elementNullEvery` control NULL containers and elements,
with defaults 8 and 7. Set both to zero for unchanged unique-list profiles, where repeated
NULL elements would otherwise force a gather and hide the lazy-allocation benefit.
These controls extend the release benchmark; they do not establish a whole-job speedup.

Six 5,003-row fixture smoke checks pass on released Flink 2.2.1: Boolean and string short
and large lists, repeated wide strings, and truly unique non-null string/integer lists.
Those one-trial startup-dominated durations validate execution and route assertions only.
Repeated release performance measurements remain required for the new element types.

## Fixed-binary boundary CPU profile

Released async-profiler 4.5 records 20 seconds of repeated two-million-row native fixed-ELT
jobs after two warmups, using CPU sampling at 1 ms, production mimalloc, both transposes
and the runtime source/blackhole sink. Of 30,955 samples, 16,010 have a Flink StreamTask frame.
The Rust ELT evaluator is the leaf in 174 task samples (1.09%). Row-to-Arrow accounts for
56.01% inclusive, native Calc including its chained exit 42.88%, and Arrow-to-row including
its downstream copy 23.77%. These inclusive fractions overlap and must not be added.
[Profile counts](../benchmarks/fixed-binary-profile-2026-10-01.csv) characterize this workload;
other shared-host jobs were active, so this is attribution evidence rather than throughput
admission. The small kernel share redirects copy-removal work toward conversion and copying
at the row boundaries rather than another ELT kernel rewrite.


### Boolean whole-job diagnostic (2026-10-01)

At revision `6160a21b`, a release/mimalloc Flink 2.2.1 comparison of
`ARRAY_DISTINCT(arr)` uses two million runtime rows, 64 Boolean elements, nullable
containers every eight rows and nullable elements every seven positions. Parallelism
is one, the transpose batch is 1,024 rows, the JDK is 17 and the maximum heap is 8 GiB.
Both row/Arrow transposes, JNI and a blackhole row sink remain in the measured native
path, with plan and runtime checks. Two warmups precede five alternating measured
trials. Stock Flink's median is 3.428 s and native's 2.770 s (1.24x), but ranges are
3.187–4.344 s and 1.820–3.036 s respectively. Other jobs and a native build were active;
this is diagnostic evidence, not a controlled performance gate. The previous-production
comparison remains pending. [All trials](../benchmarks/array-distinct-boolean-wholejob-2026-10-01.csv)
are retained.

The 200,000-row, 64-element unique STRING comparison failed with a TaskManager
heartbeat timeout during severe shared-host memory pressure; it yields no timing result.
A retry uses a 2 GiB heap to bound this diagnostic's resource use. No speedup is inferred
from the failed run and no timeout or production setting was changed to hide the failure.


The 2 GiB retry completes with all plan/runtime checks: stock median 7.360150 s
(range 6.975453–8.098612 s), native median 7.358441 s (6.813871–9.192524 s),
effectively 1.00x. It uses 200,000 rows, 64 unique non-null strings per array,
264 suffix bytes plus the UTF-8/NUL/index prefix, two warmups and five alternating
trials. All other settings match the Boolean diagnostic. Other jobs were active;
[all retry trials](../benchmarks/array-distinct-string-unique-wholejob-2026-10-01.csv)
are retained. Reduced native gather allocations alone have not demonstrated an
end-to-end win for this wide unique-string workload. Do not promote the draft
admission based on the allocation witness.


The matching previous-production run uses `1b1b5ed8` (production main plus benchmark
foundation), with only the current fixture copied into the checkout. Planner and
runtime checks confirm the string expression falls back. The same 2 GiB configuration
produces stock median 7.418640 s (7.032609–8.228943 s) and previous StreamFusion
fallback median 6.951133 s (6.761622–7.371126 s). Candidate native's 7.358441 s
median therefore fails to demonstrate improvement against either required baseline.
Runs were sequential but other host jobs remained active; [all previous-version trials](../benchmarks/array-distinct-string-unique-previous-2026-10-01.csv)
are retained rather than treating this cross-run difference as a precise regression
estimate. The draft performance blocker remains.
