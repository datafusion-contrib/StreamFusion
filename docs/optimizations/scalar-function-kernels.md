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
binary projections or identity controls, nor by itself measure a previous-StreamFusion composition baseline; the later
paired comparison below adds that missing evidence. The standalone performance requirement remains unresolved.

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


### Fixed-BINARY exit projection experiment (2026-10-01)

A local ablation at `888f9ba2` removes BINARY/VARBINARY from the generated binary-row
exit projection, retaining the generic Arrow-backed row and Flink's normal downstream
copy. This changes only the two type entries in the projection whitelist; it does
not change row ownership or bypass either transpose. The same release native library
is retained. Measurements use released Flink 2.2.1/JDK 17, a 2 GiB heap, parallelism
one, two million rows, 1,024-row batches, two warmups and five alternating trials.
The query selects `ELT(n,b,X'00112233445566778899AABBCCDDEEFF')` from the fixed-byte
runtime source into a blackhole row sink; native plan/runtime checks include both
transposes. Source-matched fixed-byte identity controls are also retained.

With the generic exit, ELT medians are 0.331388 s stock and 0.404879 s native
(0.82x). Restoring the current generated projection gives 0.287855 s stock and
0.384462 s native (0.75x). Both lose to stock, and differing stock times plus a
4.36-second identity outlier expose shared-host variance; these sequential runs do
not establish a precise projection-only difference. [Generic-exit trials](../benchmarks/fixed-binary-generic-exit-2026-10-01.csv)
and [restored-projection trials](../benchmarks/fixed-binary-projected-exit-2026-10-01.csv)
retain every measurement, including the unfavorable identity results. The ablation
is reverted; production source matches the original generated projection. It does
not resolve the BINARY admission blocker or justify changing the default exit path.


A further configured-batch diagnostic retains the restored projection and sets
`streamfusion.transpose.batchRows=16384`, changing no default. Other query,
source/sink and measurement settings match the 2 GiB projection experiment.
ELT medians are 0.322973 s stock and 0.361688 s native (0.89x), still below the
required stock throughput. Identity medians are 0.280493 s and 0.570996 s, with a
4.54-second native outlier. [Every larger-batch trial](../benchmarks/fixed-binary-batch16384-2026-10-01.csv)
is retained; shared-host variation prevents attributing the cross-run difference
precisely to batching. Larger batches do not resolve this workload's performance
gate, and the default remains 1,024 rows with the existing latency backstop.


### Boolean previous-production comparison (2026-10-01)

A fresh paired comparison uses the same two million runtime rows, width 64,
Boolean domain two, container NULL every eight rows and element NULL every seven
rows. Both release runs retain two warmups, five alternating trials, a 2 GB JVM
heap, the runtime row source/blackhole sink and all actual execution costs. The
previous-production checkout changes only benchmark fixtures and verifies the
expected expression fallback; the candidate verifies native Calc and both
transposes at planning and runtime.

| Revision | Stock Flink median / range (s) | StreamFusion median / range (s) |
|---|---:|---:|
| Previous production | 2.122 / 1.926–2.148 | 2.040 / 1.955–2.060 (fallback) |
| Candidate | 1.945 / 1.920–1.977 | 1.502 / 1.488–1.542 (native) |

The candidate beats its same-run stock control by 1.294x and the measured previous
fallback by 1.358x. Stock timing also changes between runs, so these are shared-host
samples rather than proof of a quiet-host gate or all cardinalities. The earlier
wide-variance diagnostic remains retained.
[All fresh comparison trials](../benchmarks/array-distinct-boolean-recomparison-2026-10-01.csv)
include both stock controls; STRING still lacks a demonstrated win against both
baselines and the remaining collection coverage/performance gaps remain open.


### Paired BINARY composition comparison (2026-10-01)

The current candidate and the previous-production checkout repeat the existing
grouped-COUNT fixtures at two million rows, a 264-byte source budget and non-null
inputs. Both use release libraries, a 2 GB JVM heap, 1,024-row transpose batches,
two warmups and five alternating trials. Candidate plans require native Calc,
native columnar aggregation and both transposes; runtime substitution is verified.
Previous production changes only fixtures and verifies fallback for the unsupported
expression pipeline. Row sources and blackhole sinks remain in every measured run.

| Group key expression | Candidate-run stock (s) | Candidate native (s) | Previous fallback (s) | Previous-run stock (s) |
|---|---:|---:|---:|---:|
| String to BINARY(16) | 1.235 | 0.977 | 1.225 | 1.222 |
| BINARY(16) ELT | 0.644 | 0.442 | 0.633 | 0.641 |

Candidate ranges are 0.963–0.997 seconds for cast and 0.426–0.445 seconds for ELT.
Same-run stock ranges are 1.232–1.250 and 0.628–0.664 seconds; previous fallback
ranges are 1.213–1.245 and 0.625–0.692 seconds. These sampled complete pipelines
beat both baselines: 1.264x / 1.456x against same-run stock and 1.254x / 1.432x
against previous fallback. They support the composed native island, not standalone
BINARY projection acceleration. All identity controls and trials are retained in
[candidate results](../benchmarks/binary-composition-candidate-2026-10-01.csv) and
[previous results](../benchmarks/binary-composition-previous-2026-10-01.csv). Shared
host conditions and the separately documented standalone losses remain relevant
limits; fixed-BINARY admission/performance work stays draft.


### Duplicate-heavy STRING comparison (2026-10-02)

The paired release comparison uses 200,000 runtime rows, array width 64,
string domain eight, a 264-byte suffix with Unicode/NUL prefix, container NULL
every eight rows and element NULL every seven rows. Both revisions use two
warmups, five alternating trials and a 2 GB heap. The candidate verifies native
Calc and both transposes at planning and runtime; previous production verifies
expression fallback. All conversion and JNI costs remain in the measured path.

| Revision | Stock Flink median / range (s) | StreamFusion median / range (s) |
|---|---:|---:|
| Previous production | 3.152 / 3.119–3.180 | 3.172 / 3.149–3.178 (fallback) |
| Candidate | 3.295 / 3.282–3.317 | 3.494 / 3.440–3.579 (native) |

The candidate takes 6.0% longer than its stock control and 10.1% longer than
the previous fallback sample. Stock controls also drift by 4.5% between runs.
This duplicate-heavy workload fails the performance gate, extending the existing
unique-string limitation; native STRING coverage remains draft.
[All 20 comparison trials](../benchmarks/array-distinct-string-duplicates-2026-10-02.csv)
retain both stock controls and unfavorable results.


### Duplicate-heavy STRING CPU diagnostic (2026-10-02)

An async-profiler 4.5 CPU recording repeats the duplicate-heavy workload above
with the same saved release/mimalloc library, released Flink 2.2.1 and JDK 17.
This diagnostic uses one warmup and two alternating trials; its instrumented
timings are not performance-gate evidence. Sampling uses a 1 ms interval across
the test JVM, including startup, warmup and both engines. Analysis selects the
native operator task threads and removes the thread-name frame before matching
operator stacks: names themselves contain the full chained operator plan.
There are 10,314 native-task CPU samples.

| Inclusive stack match | Native-task samples | Share |
|---|---:|---:|
| Flink object-array conversion | 5,805 | 56.3% |
| Flink UTF-8 materialization | 4,488 | 43.5% |
| Entry transpose | 3,537 | 34.3% |
| Native Calc | 1,649 | 16.0% |
| Rust ARRAY_DISTINCT | 1,040 | 10.1% |
| Exit transpose | 381 | 3.7% |

These inclusive counts overlap and must not be added. Source-side Flink
conversion/materialization dominates this witness; removing a small exit copy
alone would not explain a large end-to-end win. The source and its wire conversion
remain in the benchmark. Kernel membership hashing still merits a bounded
experiment, with whole-job comparison required afterward.
[All matched sample counts](../benchmarks/array-distinct-string-cpu-2026-10-02.csv)
retain boundary and serializer details. Raw JFR and collapsed stacks are retained
in the task artifacts. A first recording was overwritten by a later Maven JVM
and is excluded; profiling outputs now include the process ID.


### Rejected adaptive membership experiment (2026-10-02)

Three removed variants keep the first eight distinct values on the stack even for
wide arrays, promoting to the existing hash set on the ninth value. They use
length-only, four-byte and eight-byte prefix fingerprints; equality always checks
the full value. Release Criterion compares 1,024-row, width-64 STRING batches
with domain four or 64, 264-byte suffixes and both NULL profiles. Original runs
precede each variant; each has one-second warmup, 30 samples and a two-second
measurement target on the shared host. All fixture output assertions pass.

The duplicate-heavy comparisons improve by roughly 50–60%, but non-null unique
strings regress by 6–9% in Criterion's comparison estimates. Nullable unique cases
vary from a small regression to an improvement. The final eight-byte variant
reports non-null unique time 1.917 ms (95% interval 1.883–1.948 ms), versus the
original 1.858 ms (1.759–1.947 ms). Criterion's distribution comparison reports
+8.45% (3.26–14.26%); that estimator differs from the displayed point timings.
This is a cardinality tradeoff, not a general STRING acceleration. No production
change is retained and no new whole-job win is claimed.
[Mean/slope estimates and confidence intervals for every variant](../benchmarks/array-distinct-adaptive-membership-prototype-2026-10-02.csv)
retain unfavorable controls. The per-profile allocations also remain in task logs.
See the scoped [rejection](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/.claude/wontdos/array-distinct-adaptive-membership.md).


### Exact Boolean membership bits (2026-10-02)

Boolean ARRAY_DISTINCT tracks `false` and `true` in two bits, with NULL handled
separately. The caller selects this path at compile time; integer and string
loops retain collision-checked membership. There is no hash allocation or equality
scan for Boolean membership, including wide arrays. Selection order, lazy gather
allocation, sliced inputs and declared child/container nullability are unchanged.

Release Criterion repeats 1,024-row Boolean fixtures at widths eight and 64,
with/without NULLs, two-second warmup, 50 samples and a three-second measurement
target. Original measurements precede the candidate on the same shared host.
All 84 collection output/allocation assertions pass in the final comparison.

| Width / NULL profile | Original estimate (µs) | Bit membership estimate (µs) |
|---|---:|---:|
| 8 / non-null | 21.012 | 17.286 |
| 8 / nullable | 23.086 | 21.050 |
| 64 / non-null | 205.950 | 95.898 |
| 64 / nullable | 188.310 | 113.200 |

Criterion's distribution comparisons report improvements of 18.0%, 8.9%, 53.6%
and 39.2%, respectively. These are kernel witnesses, not whole-job speedups.
[All original, stack-only, runtime-bit and compile-time-bit mean/slope estimates](../benchmarks/array-distinct-boolean-membership-2026-10-02.csv)
retain confidence intervals and the intermediate stack-only small-array regressions.
All nine SQL parity checks pass against both released Flink 2.2.1 and 1.18.1
with the final release/mimalloc library. They cover Boolean/string equality,
integer widths, nested DISTINCT/lookup, NULLs, schema and unsupported-type fallback.

Fresh whole-job comparisons retain two million runtime rows, domain two,
container NULL every eight rows and element NULL every seven rows, a 2 GB JVM
heap, 1,024-row batches, two warmups and five alternating trials. Candidate plans
and runtime checks require native Calc and both transposes; previous production
verifies whole-expression fallback. Source/blackhole and all conversion costs
remain measured. The candidate identifier is parent `3dc219af` plus this Boolean
bit specialization; previous production is `1b1b5ed8` with fixture-only updates.

| Width / revision | Stock median / range (s) | StreamFusion median / range (s) |
|---|---:|---:|
| 64 / previous production | 1.884 / 1.866–1.906 | 1.878 / 1.866–1.884 (fallback) |
| 64 / candidate | 1.887 / 1.861–1.901 | 1.264 / 1.258–1.297 (native) |
| 8 / previous production | 0.513 / 0.499–0.516 | 0.505 / 0.503–0.518 (fallback) |
| 8 / candidate | 0.568 / 0.556–0.676 | 0.568 / 0.548–0.575 (native) |

Width 64 beats stock and previous fallback by 1.493x and 1.486x, respectively.
Width eight ties its same-run stock control but takes 12.7% longer than the
previous fallback sample. Its stock control also moves by 10.7% between runs;
this variability is retained rather than used to claim a small-array win.
The shared-host small-array comparison does not pass both performance baselines.
[All 40 whole-job trials](../benchmarks/array-distinct-boolean-bits-wholejob-2026-10-02.csv)
include both stock controls. STRING and standalone BINARY gates also remain
unresolved; the PR stays draft. Kernel gains alone do not complete those gates.


### Small Boolean array follow-ups (2026-10-02)

To investigate the width-eight stock-control drift, fresh runs put previous
production before the candidate, reversing the earlier revision order. A second
pair increases the row count to ten million while retaining width eight, domain
two, the same NULL profiles, two warmups, five alternating trials, release/mimalloc,
2 GB heap and both rowwise boundaries. Candidate native plan/runtime and previous
fallback checks pass in all four runs; fixture sources and Maven POMs are identical
between the checkouts.

| Rows / revision | Stock median / range (s) | StreamFusion median / range (s) |
|---|---:|---:|
| 2m / previous first | 0.513 / 0.509–0.546 | 0.525 / 0.493–0.536 (fallback) |
| 2m / candidate second | 0.541 / 0.532–0.643 | 0.544 / 0.537–0.623 (native) |
| 10m / previous first | 2.237 / 2.197–2.281 | 2.246 / 2.206–2.291 (fallback) |
| 10m / candidate second | 2.491 / 2.469–2.584 | 2.497 / 2.456–2.698 (native) |

The two-million-row candidate takes 3.6% longer than the previous fallback
sample; the ten-million-row candidate takes 11.2% longer. Both tie their own stock
controls, whose medians also differ between revisions by 5.5% and 11.4%. These
measurements cannot separate shared-host/JVM effects from revision differences,
and no adjustment is used to claim an acceleration. Increasing workload duration
does not establish a small-array win against both baselines.
[All 40 follow-up trials](../benchmarks/array-distinct-boolean-small-followups-2026-10-02.csv)
retain the unfavorable results alongside the original comparison and wide-array
win. The unresolved signal warrants boundary profiling rather than further
kernel-only claims or silently enabling the feature as complete.


### Small Boolean array boundary profile (2026-10-02)

An async-profiler 4.5 CPU recording uses ten million rows with the same width-eight
NULL/domain configuration and saved final release/mimalloc library. One warmup and
two alternating trials provide a diagnostic, not gate timings. The 1 ms recording
includes both engines; analysis selects native operator task threads and excludes
the thread-name frame before matching stacks. Of 7,955 native-task CPU samples,
Rust ARRAY_DISTINCT accounts for 689 (8.7%), row serialization for 2,691 (33.8%),
and the exit transpose for 1,709 (21.5%). Entry transpose 4,475 (56.3%) and Native
Calc 3,056 (38.4%) include downstream chained execution: inclusive counts overlap
and cannot be added. [All sample counts](../benchmarks/array-distinct-boolean-small-cpu-2026-10-02.csv)
retain source/array-writer detail; raw JFR and collapsed stacks remain in task
artifacts. Process-specific filenames prevent later Maven JVMs overwriting them.

A removed prototype extends the existing generated binary-row projection to
Boolean arrays. Reference-first inspection of Comet's `CometColumnarToRowExec`
confirms generated row projection at this boundary; the prototype uses released
Flink projection/array serialization and preserves the existing owned-copy rules.
It passes 26 checks on Flink 2.2.1, including object reuse on/off, NULL containers
and elements, empty arrays, all row kinds, multiple closed Arrow batches, 8,192
elements to force payload growth, and the nine existing SQL parity checks.

The ten-million-row release comparison retains two warmups, five alternating
trials and both transposes. Prototype median is 2.362853 s (2.347223–2.364031),
stock 2.281326 s (2.246154–2.336978): 3.6% slower. It also takes 5.2% longer than
the preceding previous-production fallback sample (2.245811 s). Earlier candidate
controls differ, so the lower absolute native time is not a controlled improvement.
[All ten prototype trials](../benchmarks/boolean-array-exit-prototype-2026-10-02.csv)
are retained. Code and prototype-only ownership tests were removed after this
failed gate; Flink 1.18 prototype validation was not run. This rejects the specific
projection extension, not future ownership-preserving boundary optimizations.

## Synchronous entry follow-up

The measurements above precede the consumer-local entry serializer. The
[entry transpose ledger](projection-pruning-transpose.md#borrowing-at-the-synchronous-consumer)
records the updated complete-job comparisons and all trials. The non-null standalone
STRING-to-fixed-BINARY cast now beats stock and previous production, and the small nullable
Boolean ARRAY_DISTINCT case does so in two sustained candidate runs. Standalone ELT remains
slower, including at ten million rows. Duplicate-heavy STRING ARRAY_DISTINCT now takes 3.160 seconds native versus 3.150 stock;
overlapping trials do not establish a win against both baselines. The [quiet-host unique-string comparison](#quiet-host-unique-string-control-2026-10-02)
below subsequently measures improvement against both baselines. Duplicate-heavy coverage
still lacks a demonstrated win. Earlier rejected exit projections and membership
experiments remain rejected.

### Preserve ELT literal cardinality

ELT's released DataFusion scalar adapter previously repeated each binary literal into an
array as long as the runtime batch. Mixed scalar/array calls now use `AcceptsSingular` for
value arguments: the index still supplies the batch-length loop, and a literal's bytes or
NULL are borrowed from its one-row adapted array once. Selection writes only the chosen
bytes into the declared output builder. Original argument representation identifies a
scalar; array length one never implies scalar semantics. All-scalar calls keep the released
adapter's scalar extraction, and array-only/all-scalar calls allocate no hint vector.

This preserves the existing cast and output-width checks, invalid-index/NULL behavior and
owning output buffers. It changes neither JNI nor the row/Arrow boundary. Release Rust
tests cover sliced binary values and 48 empty/one-row mixed-call combinations, including
scalar versus array indices and NULL literals; the Criterion matrix checks 960 complete
values/types and all-scalar representation contracts.

For a non-null 1,024-row width-16 dynamic-index call with a variable-binary literal,
requested Rust bytes drop from 37,972 to 17,579, while allocation calls increase from 17
to 18 and the 16,512 output Arrow buffer bytes stay unchanged. The removed allocation
volume is repeated literal storage, not a measurement of every copied byte. A revised
candidate averages 4.992 microseconds against a fresh published-code control's 5.724,
but pre-expanded controls drift by a similar amount (4.883 versus 5.535); these sequential
measurements do not support a precise throughput percentage. An initial unconditional-hint
version is replaced because its hint allocation is unnecessary outside mixed calls.

The [Criterion methodology and retained evidence](../benchmarks/native-criterion.md#scalar-preserving-adapter-measurements)
include all 960 allocation probes, 12 estimates with 95% intervals and 1,200 raw samples,
including both candidates and the published-code control. This bounded kernel change alone
does not resolve the previously measured standalone ELT deficit against stock Flink.

Fresh release/mimalloc whole-job runs on Flink 2.2.1/JDK 17 use the identical row-fed
`ELT(n,b,X'00112233445566778899AABBCCDDEEFF')` query, BINARY(16) source/result, blackhole
sink, one worker, 2 GiB heap, batch limit 1,024, two warmups and five measured runs per engine.
Plans require NativeCalc and both transposes, with runtime substitution checks. The
pre-kernel control uses the saved production library from before scalar preservation;
previous production uses main `0b38269e` plus benchmark-only foundation `1b1b5ed8`, its
matching Java/native code, and explicit expected-ELT-fallback checks. Source/query fixtures
in the previous-production worktree are byte-identical to the current diagnostic fixtures.

| Rows / source NULL interval | Revised native / stock seconds | Before kernel native / stock | Previous-production fallback / stock |
| --- | ---: | ---: | ---: |
| 10M / none | 1.272 / 1.009 | 1.280 / 1.019 | 1.198 / 1.199 |
| 2M / every seventh | 0.316 / 0.284 | 0.325 / 0.282 | 0.310 / 0.313 |

[All 120 identity/function trials](../benchmarks/binary-elt-singular-whole-job-2026-10-02.csv)
are retained. Ten-million-row native function ranges overlap (revised 1.266–1.288 seconds,
pre-kernel 1.274–1.331); its identity medians also shift by roughly eight milliseconds
(1.159 versus 1.167), limiting attribution of the function difference to this kernel.
Nullable two-million-row function ranges overlap too (0.314–0.321 versus 0.317–0.330).
Previous-production stock controls drift substantially, so they cannot establish a precise
cross-worktree speed ratio. Revised ELT still loses to its own stock control and the
previous-production fallback in both shapes. The change is retained for eliminating
batch-length literal storage, with no claimed standalone acceleration or resolved admission
gate. Release fixed-BINARY SQL checks pass 17/17 on Flink 2.2.1 and 11/17 on 1.18.1,
with six explicit unavailable-stock-ELT skips. The added runtime-source cases cover widths
1/16/256 across complete and partial batches, NULL literals, padding/truncation of hex and
UTF-8 literals and a narrower declared NULL-literal width. All 960 final Criterion smoke
fixtures and three release Rust tests pass.

With the scalar diagnostic's configurable width set to 256, two million non-null source
rows give ELT native/stock medians 0.435/0.307 seconds versus the pre-kernel control's
0.448/0.304 and previous-production fallback/stock 0.336/0.350. Candidate and pre-kernel
native ranges overlap (0.433–0.449 versus 0.441–0.451). Native identity controls are
0.451/0.444 seconds, so the function difference is not simply an identical shift in the
identity cost; its magnitude remains uncertain. [All 60 wide identity/function trials](../benchmarks/binary-elt-singular-wide-whole-job-2026-10-02.csv)
are retained, with matched schema/literal widths and the same resources/warmups/repetitions
as above. Width 256 still fails the standalone stock and previous-production performance
gate; no admission or default change is justified by these measurements.


### Quiet-host unique STRING control (2026-10-02)

A fresh sequential comparison uses the same 200,000-row, width/domain-64,
264-byte-suffix non-null STRING fixture, released Flink 2.2.1, JDK 17, a
2 GiB heap, two warmups and five alternating stock/native trials. No other
local compilation or timing jobs overlap the measured trials. Both checkouts
use byte-identical fixtures; native uses the saved production release/mimalloc
library rather than the Criterion counting-allocator library. Plan checks retain
the row-fed source, blackhole sink and both transposes, and runtime checks verify
acceleration for the candidate and fallback for previous production.

Candidate median is 5.906901 seconds (5.712531–5.939868), versus its stock
control at 6.427183 (6.420305–6.460492): 8.1% less elapsed time. Previous
production fallback measures 6.255865 (6.168646–6.276111), with its stock
control at 6.220809 (6.163930–6.361735). Candidate is 5.6% faster than the
previous fallback median. The stock controls differ by 3.3%, so retain both
and do not interpret the sequential comparison as a precise universal gain.
[All 20 trials and actual native-library hashes](../benchmarks/array-distinct-string-unique-quiet-2026-10-02.csv)
are retained. This supplies positive evidence for the unique-string workload;
it does not erase the earlier confounded results, demonstrate a win for the
duplicate-heavy profile, or complete the collection-family admission gate.


### Quiet-host duplicate STRING control (2026-10-02)

The matching duplicate-heavy comparison keeps 200,000 rows, width 64, suffix
264, two warmups, five alternating trials, 2 GiB heap and the same release
libraries. Domain is eight, containers are NULL every eight rows and elements
NULL every seven positions. Candidate and previous production run sequentially
with the same plan/runtime route checks and no overlapping local benchmark or
compilation jobs during measured trials.

| Revision | Stock median / range (s) | StreamFusion median / range (s) |
| --- | ---: | ---: |
| Candidate | 3.339411 / 3.300283–3.366571 | 3.271065 / 3.173405–3.417530 |
| Previous production | 3.142147 / 3.140216–3.212720 | 3.169808 / 3.152526–3.214228 (fallback) |

Candidate's median is 2.0% lower than its stock control, with overlapping trial
ranges, and 3.2% higher than previous production. Stock controls drift 6.3%
between runs. This does not demonstrate improvement against both baselines and
keeps the duplicate-heavy STRING performance gate unresolved. The unique-string
win above must not be generalized into an unrestricted STRING admission claim.
[All 20 duplicate-heavy trials and native-library hashes](../benchmarks/array-distinct-string-duplicates-quiet-2026-10-02.csv)
retain the unfavorable control. No production kernel change is made by these
remeasurements.


### Remaining STRING exit copies

The current Arrow VARCHAR column adapter obtains a heap byte array from
`VarCharVector.get` for every string access. Released Flink 2.2.1 and 1.18.1
`RowDataSerializer` copy each field through its type serializer;
`ArrayDataSerializer` similarly copies columnar-array elements, and
`StringDataSerializer` calls `BinaryStringData.copy`. Thus retained string
outputs pay for the temporary Arrow-to-heap bytes and the subsequently owned
string bytes. This finding comes from inspecting the released artifact
bytecode, rather than assuming the development checkout matches either release.

Flink's `ColumnarRowData` and `ColumnarArrayData` are final on both releases.
An alternate borrowed-string row view would need a different implementation and
may lose the existing columnar-array serializer branch. Removing the temporary
copy therefore needs a measured design that preserves synchronous consumption,
retained rows after batch close, fan-out and network serialization, rather than
changing the existing adapter to expose a buffer whose lifetime it cannot
guarantee. No ownership change or allocation saving is claimed here. The entry
VARCHAR writer already copies heap binary-string segments directly into Arrow,
following Comet's buffer-based writer pattern; that existing optimization also
applies to nested array elements.


### Scalar-index binary ELT array reuse (prototype)

The candidate returns a selected immutable Arrow array directly when ELT's index
is a positive INT scalar, the selected value is an array of exactly the declared
result type, all array arguments have the expected batch length, and all value
arguments are binary. This avoids index expansion and rebuilding the selected
payload. Other indexes and type adaptation keep the original path. Array output
representation, slices and NULLs are preserved, including empty and single-row
batches. The retained Arrow reference owns its buffers after the argument batch
is dropped; no Java or native handover contract changes. Comet's existing
expression and schema-cast paths similarly return an `Arc` clone when input
already satisfies the result contract.

All four targeted release tests pass, including 12 buffer-ownership cases.
The runtime SQL width-1/16/256 fixture now also selects runtime columns with
constant first/second indexes across 1,033 rows and includes a NULL unselected
literal. The release/mimalloc candidate library passes all 32 selected Flink
2.2 tests: 17 fixed-BINARY SQL cases, 10 pruned-transpose ownership cases and
five synchronous Arrow-input cases. These include stock/native values, types
and operator checks. The matching Flink 1.18 suite passes 26 cases and explicitly
skips six unavailable-function cases; it does not prove ELT parity on that
release. Exception correctness uses full stack traces on Flink 1.18; all
three targeted Flink 2.2 failing-index, input-failure and unselected-input
short-circuit checks also pass with full stack traces enabled. `ELT_FIXED_BINARY_FIRST` adds the matching scalar-index
query to the row-fed whole-job harness, retaining its existing transposes and
blackhole sink. Eight original Criterion baselines are
retained before the change, with 100 samples each: 16,384 rows, widths 16/256,
nullable/non-null inputs and scalar/dynamic index controls. The existing full
960-profile binary suite also includes invalid/NULL indexes, literal selection,
expanded controls and all-scalar calls. The candidate passes all 960 fixture checks. Allocation requests fall in 180
profiles and remain equal in 780; none increase. Four scalar-index timing
controls fall from 62.847–203.860 microseconds to 0.127–0.134 microseconds,
removing over 99% of kernel time by returning the selected array.

The first non-null width-256 dynamic-index control regresses 4.6% (95% change
interval 3.7–5.5%), while its nullable counterpart improves 4.8%. Other dynamic
controls vary between a 1.4% improvement and a 0.3% change with an interval
including zero. Preserve this unfavorable control and repeat the dynamic
comparisons before accepting the change.
[All eight comparison estimates](../benchmarks/binary-elt-array-selection-comparison-2026-10-02.csv),
[16 original/candidate timing estimates](../benchmarks/binary-elt-array-selection-timing-2026-10-02.csv)
and [all 1,600 samples](../benchmarks/binary-elt-array-selection-samples-2026-10-02.csv)
retain the complete first comparison. The repeated and pinned controls below retain the uncertain comparisons;
whole-job validation remains pending. The standalone SQL gate uses a dynamic index and
will not be fixed by this scalar-index branch. No acceleration claim is made
from the prototype alone.

A second candidate run does not reproduce the first width-256 non-null
regression: its mean is 164.495 microseconds, a -0.4% change with a 95% interval
including zero. The nullable width-256 case instead rises to 186.696 microseconds,
+12.6% (11.8–13.3%), after improving in the first run. Narrow dynamic controls
remain close to their earlier results. These opposite wide-case outcomes leave
the dynamic comparison unresolved; do not dismiss either unfavorable run.
[All four repeat estimates](../benchmarks/binary-elt-array-selection-dynamic-repeat-comparison-2026-10-02.csv)
and [all 400 repeat samples](../benchmarks/binary-elt-array-selection-dynamic-repeat-samples-2026-10-02.csv)
are retained. The fresh pinned comparisons below extend this check with reversed run
order; acceptance still requires the remaining validation.

Two fresh rounds pin the unchanged original and saved candidate executables to
CPU 0, reverse their execution order and use the same four dynamic controls,
3-second warmups, 5-second measurement targets and 100 samples per case.

| Width / nullable | Candidate change in mean, original first | Candidate change in mean, candidate first |
| --- | ---: | ---: |
| 16 / false | -1.8% | -1.2% |
| 16 / true | -1.5% | -0.8% |
| 256 / false | -10.1% | +4.4% |
| 256 / true | -9.6% | -1.7% |

The unchanged wide non-null original varies from 178.981 to 159.990
microseconds between rounds. CPU affinity alone has not settled the wide-case
comparison; retain the +4.4% result alongside the improvements.
[All 16 paired timing estimates and confidence intervals](../benchmarks/binary-elt-array-selection-pinned-timing-2026-10-02.csv)
and [all 1,600 paired samples](../benchmarks/binary-elt-array-selection-pinned-samples-2026-10-02.csv)
are retained. Production-allocator JNI and whole-job validation are next; the
prototype remains unaccepted and the existing dynamic-index SQL gate unresolved.


The first release/mimalloc whole-job comparison uses 2 million row-fed
BINARY(16) records, NULL every seventh row, a 2 GiB heap, two warmups and five
alternating stock/native measurements. The native plan includes both transposes
and drains to blackhole. The same benchmark fixtures run against previous
production, where this query falls back. All 40 measured trials, including the
identity controls, are [retained](../benchmarks/binary-elt-scalar-wholejob-2026-10-02.csv).

| Build | Stock median / range (s) | Native or fallback median / range (s) |
| --- | --- | --- |
| Array-reuse candidate | 0.294 / 0.270–0.335 | 0.311 / 0.305–0.312 (native) |
| Previous production | 0.320 / 0.311–0.356 | 0.321 / 0.317–0.344 (fallback) |

The candidate is 5.7% slower than its stock control and 3.1% faster than
previous production. Stock controls themselves differ by 8.8%, and measured
ranges overlap. Identity medians are 0.277/0.322 seconds stock/native for the
candidate and 0.318/0.398 seconds for previous production. This narrow-payload
profile does not establish acceleration despite the kernel allocation saving.
The completed width-256 comparison retains all
[60 measured trials](../benchmarks/binary-elt-scalar-wide-wholejob-2026-10-02.csv)
under the same configuration, adding the prior candidate without array reuse.

| Build, width 256 | Stock median / range (s) | Native or fallback median / range (s) |
| --- | --- | --- |
| Array-reuse candidate | 0.331 / 0.316–0.356 | 0.432 / 0.429–0.440 (native) |
| Prior candidate without reuse | 0.341 / 0.316–0.373 | 0.473 / 0.458–0.560 (native) |
| Previous production | 0.349 / 0.336–0.363 | 0.346 / 0.338–0.369 (fallback) |

Array reuse reduces wide-query elapsed time by 8.7% against the prior candidate,
whose measured native range is higher, but remains 30.6% slower than its stock
control and 24.8% slower than previous production. Stock medians differ by up to
5.6%. Identity stock/native medians are 0.317/0.457 seconds for the candidate,
0.324/0.451 for the prior candidate, and 0.345/0.513 for previous production.
The copy removal therefore helps an existing draft kernel but does not clear
the end-to-end acceleration gate. The prototype remains unaccepted; conversion
costs require further profiling and optimization before shipping the feature.


A separate 45-second CPU profile of the width-256 candidate job, after two
warmups, records 56,885 samples using async-profiler 4.5 at a 1 ms interval.
It repeatedly executes whole jobs, so planning, startup, source conversion,
GC and shutdown remain in the recording. Classifying each stack by its deepest
entry, native Calc or exit process/flush frame avoids counting chained downstream
work twice: entry owns 10,507 samples (18.5%), native Calc 3,141 (5.5%), exit
12,516 (22.0%), and other work 30,721 (54.0%).
[Sample attribution](../benchmarks/binary-elt-scalar-wide-cpu-profile-2026-10-02.csv)
is diagnostic CPU evidence, not an elapsed-time or allocation benchmark.

Fixed-width buffer initialization under entry writer creation is a material
subset of entry samples: allocation zeroes the default capacity each batch.
This wide-payload evidence motivates revisiting batch-capacity sizing, whose
previous narrow-row experiment failed to demonstrate a speedup. The earlier
rejection remains valid for that workload; no new capacity change or win is
claimed yet. Exit projection and Arrow binary getters also copy payloads, but
previous direct-exit experiments already failed their whole-job gates.

### Scale-dependent DOUBLE TRUNCATE domain (prototype)

The proposed guard replaces the uniform magnitude limit with
`abs(value) <= 1e15 / 10^max(scale,0)` for scales -6 through 6. It preserves
the bounded decimal-coefficient limit while allowing larger magnitudes at
coarser scales, including the measured slow 1e12 workload at small positive
scales. Other domains retain released Flink evaluation. All seven Flink 2.2
oracle tests pass, including 130,234 new comparisons of large magnitudes,
scale-dependent boundaries and adjacent doubles. All seven matching Flink
1.18 oracle tests also pass. All 21 generated-evaluation and SQL cases pass
on each released version, including three 5,003-row large-boundary query forms
and preserved NULL/error/short-circuit behavior. The strengthened signed
boundary cases also pass on Flink 2.2. The matched whole-job comparison below
clears these measured profiles. All 144 release JNI fixtures also pass,
comparing exact bits, NULL positions and schemas across reflective, generated
and borrowed evaluators. The loaded candidate class is verified by bytecode
and source/class hashes. Final correctness/Javadoc, documentation and formatting
checks pass; CI and the broader issue gates remain pending.


The release/mimalloc comparison retains 2 million rows, NULL every seventh
value, a 2 GiB heap, two warmups and five alternating stock/native measurements
per query. Runtime scales range from -3 through 3 except the ambiguous profile's
fixed scale 1. The large profile alternates signed values around 1e12. Every
intended native plan executes Calc with both transposes and a blackhole sink.
Candidate and prior helper use the same native library and capacity policy;
previous production uses its saved release library and explicitly verified
fallback for these queries. Fixtures are byte-identical across worktrees.
[All 210 trials](../benchmarks/double-truncate-scale-bound-wholejobs-2026-10-02.csv)
include three source-matched identity controls for each build.

| Profile | Candidate stock / native median (s) | Prior helper native median (s) | Previous production fallback median (s) |
| --- | --- | --- | --- |
| Large | 0.903 / 0.479 | 0.945 | 0.916 |
| Bounded | 0.650 / 0.379 | 0.383 | 0.662 |
| Ambiguous | 0.607 / 0.387 | 0.394 | 0.625 |
| CASE | 0.473 / 0.396 | 0.396 | 0.489 |

Large-profile native elapsed time falls 49.3% against the prior helper, 46.9%
against stock and 47.7% against previous production. Its candidate range is
0.474–0.494 seconds, prior-helper range 0.927–0.958, stock range 0.882–0.913,
and previous fallback range 0.903–0.938. Large stock medians are 0.903, 0.895
and 0.913 seconds across builds, limiting drift to about 2%. Bounded, ambiguous
and CASE candidate ranges are 0.371–0.390, 0.385–0.390 and 0.387–0.398
seconds respectively; prior-helper ranges overlap these controls. Each beats
its stock and previous-production controls, whose complete ranges are retained
in the CSV. This fixes the measured large-domain regression and establishes a
CASE win for this configuration; it does not prove every fallback domain or
unregistered floating-function family accelerated.


### Sustained dynamic fixed-binary ELT control

The retained dynamic-index gate also runs at 20 million non-null BINARY(16)
rows, released Flink 2.2.1/JDK 17, release Rust with mimalloc, 2 GiB heap,
parallelism one, two warmups and five alternating stock/native trials. Both
transposes and the blackhole sink remain in the timed complete job. Query and
source fixtures are byte-identical in the current and previous-production
worktrees; the previous revision explicitly verifies fallback for ELT.

| Revision / order | Stock ELT median (range), s | Native-enabled ELT median (range), s |
| --- | --- | --- |
| Current, first | 2.057 (2.026–2.095) | 2.442 (2.390–2.462) |
| Previous production | 2.384 (2.356–2.425) | 2.400 (2.344–2.441), fallback |
| Current, last | 2.016 (1.984–2.080) | 2.526 (2.509–2.548) |

Current native execution loses to its matched stock controls by 18.7% and
25.3%. The identity query also loses: current native/stock medians are
2.306/1.971 seconds initially and 2.299/1.990 seconds afterward. This supports
investigating conversion costs rather than attributing the full gap to startup.
Previous-production stock controls differ substantially, so these runs do not
establish a clean cross-revision improvement or explain that variation.
[All 60 trials, including identity controls](../benchmarks/fixed-binary-elt-sustained-2026-10-02.csv)
retain unfavorable results. No new optimization or default admission is
justified; the fixed-binary dynamic ELT performance gate remains unresolved.

### Generated segment-copy exit rejected (2026-10-04)

A prototype retained Flink's generated projection while exposing batch-scoped
Arrow byte segments to the shared binary/string row writer. It removed an
intermediate payload array and preserved SQL binary types and owned output rows.
It passed 37 released Flink 2.2.1 wire, transpose and SQL cases; Flink 1.18.1
passed 31 and explicitly skipped six unavailable ELT cases.

An exit-only comparison held the release/mimalloc native library identical and
kept both transposes, a rowwise blackhole sink, 20 million nullable BINARY(256)
rows, two warmups and five measured trials. Uniform-first native medians were
3.4689 seconds candidate and 3.5598 prior; mixed medians were 3.4257 candidate
and 3.3924 prior. Stock medians ranged from 2.4059 to 2.5017. Native remained
slower than stock, mixed regressed about 1%, ranges overlapped and stock times
drifted. The small uniform improvement does not establish a safe acceleration.
The prototype is rejected and removed; the prior generated exit remains in use.
[All 80 trials](../benchmarks/generated-segment-wholejob-trials-2026-10-04.csv)
and [16 summaries](../benchmarks/generated-segment-wholejob-summary-2026-10-04.csv)
retain the identity controls and unfavorable results. This does not resolve the
fixed-BINARY performance gate or reopen the rejected manual per-field projection.
