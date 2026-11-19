# Recorded scalar workloads: collections

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## Integer ARRAY_DISTINCT (2026-09-28) {#integer-array_distinct-2026-09-28}

Measured against canonical main `665dc10d`, Intel Core i7-12650H, Linux, JDK 17,
Flink 2.2.1, release Rust with mimalloc, parallelism one, 2 GiB heap and 1,024-row
Arrow batches. Each case uses a fresh JVM, five warmups and five alternating
stock/native trials. No other build or test ran concurrently. Times include
planning and the complete job, a rowwise DataStream source, both production
transposes, JNI execution and a rowwise blackhole sink. The harness asserts
NativeCalc and both transposes, completes the job and checks planner substitution
counts; an intentionally disabled admission must have zero substitutions.

The query projects `ARRAY_DISTINCT(arr)` plus a TRUE anchor. Every eighth array is
NULL; every seventh element is NULL; other elements are
`(31 * elementIndex + rowIndex) % domain - domain / 2`. Width counts all elements;
domain is the value generator's modulus, not the resulting distinct count.

| Element type | Rows | Width / domain | Flink median (range), s | Native median (range), s | Flink/native |
| --- | ---: | ---: | --- | --- | ---: |
| BIGINT | 200,000 | 64 / 64 | 1.047 (1.039–1.065) | 0.310 (0.308–0.319) | 3.38x |
| BIGINT | 200,000 | 256 / 256 | 13.388 (13.345–13.403) | 0.924 (0.911–0.942) | 14.50x |
| BIGINT | 200,000 | 64 / 8 | 0.334 (0.328–0.373) | 0.243 (0.238–0.252) | 1.37x |
| INT | 200,000 | 64 / 64 | 0.989 (0.979–1.005) | 0.306 (0.299–0.325) | 3.23x |
| BIGINT | 2,000,000 | 8 / 8 | 0.668 (0.621–0.672) | 0.581 (0.581–0.625) | 1.15x |
| BIGINT | 20,000,000 | 8 / 8 | 5.875 (5.660–5.943) | 5.215 (5.136–5.241) | 1.13x |
| BIGINT, repeat | 20,000,000 | 8 / 8 | 5.430 (5.394–5.492) | 5.413 (5.358–5.468) | 1.00x |

Large-array gains are clear. Small-array gains are sensitive to run-to-run control
movement: the repeat is effectively tied, and neither long run is evidence of a
stable 13% improvement. Short jobs also include appreciable fixed startup costs.
These results do not establish gains for all integer widths or data distributions.

For the previous StreamFusion behavior, an admission ablation removed only the
new ARRAY_DISTINCT Java admission from the same runtime. The harness verified
`unsupported function/operator: ARRAY_DISTINCT` and zero native substitutions.
With five warmups/five trials, BIGINT 200,000 x 64/domain64 took 1.049s
(1.041–1.124), versus its paired stock control 1.093s (1.031–1.154). BIGINT
2,000,000 x 8/domain8 took 0.580s (0.545–0.607), versus stock 0.564s
(0.545–0.572). The candidate's 0.310s large-array result improves this prior
behavior by about 70%; its 0.581s small-array result is essentially unchanged
across separate JVMs. This is an admission ablation, not a rebuilt old release.

[Raw trials](array-distinct-2026-09-28.csv) retain the full investigation, including
unfavorable implementations on baseline `4aae50a1`. Released DataFusion's generic
kernel took 0.724s versus Flink 0.625s for two million eight-element arrays; typed
hash membership reduced native time to 0.659s versus Flink 0.623s. A bounded stack
search took 0.624s versus Flink 0.601s with two warmups, and 0.592s versus Flink
0.607s with five. Its twenty-million-row result remained slower (5.393s versus
5.314s). The final implementation adds a collision-checked fingerprint to that
stack search. Its first long run improved native time to 5.215s, but the Flink
control also moved; the repeat above prevents attributing the entire difference
to the kernel. CSV implementation/run columns distinguish these intermediate
variants from the shipped `typed_fingerprint` measurements.

A diagnostic async-profiler CPU-timer run of the generic kernel collected 6,519
mixed Java/native samples; 683 stacks included NativeCalc/JNI, 296 the generic
array-distinct kernel, 199 hash insertion, and 36 row conversion. These overlapping
counts include both engines and startup. That diagnostic overlapped a native
build, so its elapsed times are excluded from benchmark claims. It motivated
primitive membership without encoded rows; it does not measure isolated kernel
CPU percentages. See the [kernel technique](../optimizations/scalar-function-kernels.md#integer-array_distinct).

Reproduce each table scenario by changing rows, width, domain and `collection.int`
(the default is BIGINT):

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DynamicCollectionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx2g '-Dcollection.expression=ARRAY_DISTINCT(arr)' \
  -Dcollection.map=false -Dcollection.rows=200000 -Dcollection.width=64 \
  -Dcollection.domain=64 -Dcollection.int=false -Dcollection.warmup=5
```

For an admission ablation, remove the Java ARRAY_DISTINCT admission and add
`-Dcollection.native=false` and
`'-Dcollection.fallbackReason=unsupported function/operator: ARRAY_DISTINCT'`.
Restore admission before measuring the candidate. MAP_KEYS/MAP_VALUES were also
investigated but remain fallback after end-to-end regressions; their
[raw trials](map-array-projections-2026-09-28.csv) and
[rejection rationale](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-map-array-projections.md)
are retained.



## Array extrema admission experiment (2026-09-28) {#array-extrema-admission-experiment-2026-09-28}

Direct admission of released DataFusion 54 integer ARRAY_MIN/ARRAY_MAX kernels
was tested on baseline `2201e98f`, Intel Core i7-12650H, Linux/JDK 17, Flink 2.2.1,
release/mimalloc, 2 GiB heap, parallelism one and 1,024-row batches. Five warmups
preceded five alternating whole-job trials with a row source, row blackhole sink,
both transposes and verified native Calc routing.

For two million eight-element BIGINT arrays, MIN took 0.502s native versus 0.480s
stock and MAX took 0.496s versus 0.465s. At twenty million rows MIN took 4.549s
(4.537–4.572) versus 3.967s (3.932–4.072), a 14.7% regression. Wider arrays showed
modest gains: 200,000 arrays of width 256 took 0.497s versus 0.525s for MIN and
0.498s versus 0.532s for MAX. Width-64 BIGINT and INT results were approximately
tied or modestly favorable. The default admission was rejected; both functions
continue on Flink.

[Raw trials](array-extrema-2026-09-28.csv) include every candidate scenario and
previous-admission controls. The
[experiment note](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-array-extrema.md)
records all ranges, the input generator, reproduction commands, control drift,
prototype parity results and the separate profile. No timing from the instrumented
profile is included in the raw benchmark series. This investigation does not
remove the remaining collection gaps in [#234](https://github.com/datafusion-contrib/StreamFusion/issues/234).



## MAP scalar callback costs {#map-scalar-callback-costs}

Measured on October 20, 2026 with released Flink 2.2.1, JDK 17, release native code, parallelism 1,
2 GiB task heap and batches of 1,024 rows. `MapScalarUdfBenchmark` uses a Java identity callback
on nullable `MAP<STRING,INT>` values and a row blackhole sink. Source maps are constructed during
the job. Timings include execution planning/deployment, both perimeter transposes, JNI/C Data,
callbacks and the sink; initial environment/explain setup is excluded. These are complete-job
cost diagnostics under the accepted UDF exception, not small-functionality admission timings.

Two warmups and five measured trials alternate stock/native-enabled order. Candidate and previous-
fallback processes run serially, each with paired stock controls. Native plans require both
transposes and actual NativeCalc input counts; previous fallback requires zero native operators.

| Rows / entries | Candidate's stock control (s) | Native callback (s) | Previous fallback (s) | Previous stock control (s) |
| --- | ---: | ---: | ---: | ---: |
| 16,384 / 4 | 0.076014 | 0.163299 | 0.080867 | 0.080237 |
| 65,536 / 64 | 0.617199 | 1.221389 | 0.614807 | 0.607610 |

The callback path is about twice as slow on these fixtures. Correct external MAP coverage now
defaults on without a new opt-in; this makes no standalone acceleration claim. The blackhole
timings do not validate result values: independent SQL/Table API, mutable/retained Java values,
error/lifecycle, separate-JVM and two-restore checks do that on both released Flink lines.
The [raw evidence](results/map-scalar-udfs-2026-10-20.json) retains all trials and ranges,
paired controls, library identity, test counts, 48 transport fixtures and allocation probes.

Reproduce with `SF_BENCHMARK=true`, `-Pbench`, `-Dtest=MapScalarUdfBenchmark`,
`-Dmapbench.rows=16384 -Dmapbench.width=4` or `65536/64`, and
`-Dsf.extraJvmArgs="-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false"`. The old revision uses
the same benchmark source with `-Dmapbench.expectNative=false`.


## Exact numeric ARRAY casts {#exact-numeric-array-casts}

The functionality gate compares released Flink 2.2.1's generated Calc with the previous
StreamFusion fallback at `d951a838` and the new Rust ARRAY cast. Measurements use JDK 11,
Apple M4 Pro, release Rust with mimalloc, one active benchmark, 131,072 rows, five warmups
and nine measured trials. The Arrow input and Flink BinaryRowData contain identical integer
or DECIMAL values, NULL containers, empty arrays and NULL elements. Every trial consumes
every output element; all three engines have identical checksums for each shape.

This is the repository's small-functionality boundary: stock/previous start on prepared
RowData in Java, and native starts on prepared Arrow batches of 1,024 rows in Rust. Setup,
input IPC loading, initial/final JNI and perimeter transposes are outside timing. Generated
evaluation, native Calc evaluation, allocation and equivalent output consumption are inside.
These measurements do not establish an end-to-end job speedup. DECIMAL rescale is
DECIMAL(18,2) to DECIMAL(18,1); integer-to-DECIMAL targets DECIMAL(18,2).

Median milliseconds; speedup is stock divided by Rust:

| Conversion / array width | Stock | Previous fallback | Rust | Speedup |
| --- | ---: | ---: | ---: | ---: |
| INT to BIGINT / 4 | 10.003 | 6.681 | 0.992 | 10.08x |
| INT to BIGINT / 32 | 17.828 | 17.958 | 6.629 | 2.69x |
| INT to BIGINT / 128 | 58.470 | 60.420 | 24.851 | 2.35x |
| INT to DECIMAL / 4 | 10.589 | 13.155 | 2.322 | 4.56x |
| INT to DECIMAL / 32 | 50.672 | 37.684 | 16.713 | 3.03x |
| INT to DECIMAL / 128 | 203.839 | 186.003 | 68.721 | 2.97x |
| DECIMAL rescale / 4 | 10.629 | 10.119 | 3.426 | 3.10x |
| DECIMAL rescale / 32 | 49.437 | 45.040 | 24.884 | 1.99x |
| DECIMAL rescale / 128 | 178.626 | 152.197 | 98.925 | 1.81x |

[Raw warmup/measured times, ranges and checksums](results/exact-array-casts-2026-10-22.json)
retain the JVM trial variation; short stock trials are noisier than the Rust batch loop.
The initial sliced-input prototype repeatedly converted the complete shared child buffer.
Restricting conversion to each slice's referenced values removes that amplification, including
at nested array levels.

Run the Java half with:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench \
  -Dtest=ArrayCastAdmissionBenchmark -Darray.cast.mode=stock \
  -Darray.cast.shape=int-bigint -Darray.cast.rows=131072 -Darray.cast.width=32 \
  -Darray.cast.warmups=5 -Darray.cast.runs=9 -Darray.cast.export=/tmp/array-cast.arrow test
```

Use `int-decimal` and `decimal-rescale` for the other shapes. Copy the same benchmark test
to a separate checkout of the baseline and use `-Darray.cast.mode=previous` to require the
old planner's whole-Calc ARRAY cast fallback. The new benchmark class is disabled unless
`SF_BENCHMARK=true`. Build and run the native half on the exported input:

```sh
cd native
cargo build -p streamfusion --release --no-default-features --features mimalloc --example array_cast
target/release/examples/array_cast /tmp/array-cast.arrow 5 9
```

The native example uses the production Calc decoder and cast, times inside Rust and verifies
the same output digest. Ordinary SQL regressions separately establish released-Flink result
and type parity and positive consumed native work on both supported Flink lines.
Numeric ARRAY TRY_CAST invokes the identical total conversion kernel; these measurements use
CAST SQL and do not claim a separate TRY_CAST measurement.


## Standalone collection programs {#standalone-collection-programs}

Standalone ARRAY/MAP/ROW casts and dynamic or empty-separator SPLIT can now select the existing
generated collection evaluator without another collection function in the Calc. This is coverage
under the accepted collection-extension tradeoff; these new standalone forms are slower than
stock Flink and previous-main fallback. Existing numeric ARRAY kernels remain unchanged.

Measured on 2026-10-23 with Linux x86-64/WSL2, Intel Core i7-12650H, OpenJDK 17.0.20.1,
Flink 2.2.1 and release/mimalloc. Each query uses 500,000 runtime-source rows, parallelism 1,
an 8 GiB test heap, two warmups and three alternating measurements per engine. Source, blackhole sink, both
row/Arrow transposes, planning and submission remain timed. Initial explain and cluster setup
are outside timing. No other tests or benchmarks ran concurrently. Previous main is `1bd9daa8`,
in a separate JVM using the same native DSO and benchmark source; each new expression must
have zero native Calc substitutions there. Identity controls still require native Calc.

DECIMAL arrays contain two DECIMAL(38,9) values and a NULL element. Every eighth container is
NULL. SPLIT uses a 32-byte Unicode payload budget and every eighth input is NULL; the dynamic
separator is `LEFT(s,1)`. Empty separators produce UTF-16-unit elements, including isolated
surrogate halves, whose Java caches must survive the represented Arrow result.

Median seconds; ranges and both paired stock controls are retained in the
[trial/profile summary](results/standalone-collections-2026-10-23.json).

| Expression | Stock | Generated collection | Previous fallback | Stock / generated |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL ARRAY to STRING ARRAY | 0.397833 | 0.681770 | 0.400672 | 0.584x |
| SPLIT with dynamic separator | 0.304137 | 0.575999 | 0.294347 | 0.528x |
| SPLIT with empty separator | 0.661662 | 1.967336 | 0.650776 | 0.336x |

These complete jobs include startup; the subsecond results do not establish sustained throughput.
The initial DECIMAL-array-to-FLOAT diagnostic was already native on previous main and is retained
as a diagnostic, not evidence of newly admitted coverage or an improvement from this change.

Optimization investigation was limited to two eight-second CPU captures, one per SPLIT form.
The empty-separator capture has 10,331 samples across all JVM threads, including 2,309 inclusive
samples in Flink's array copy and 3,666 in represented array writing; the dynamic capture has
13,333 total samples, with 943 and 1,286 in those paths. Counts overlap, include compilation and
planning, and do not measure allocations or copied bytes. These paths preserve released array
copy categories and UTF-16 caches; no shortcut was enabled without a verified representation
contract. Further standalone optimization remains in [#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=DECIMAL_ARRAY_TO_STRING,SPLIT_DYNAMIC,SPLIT_EMPTY \
  -Dscalar.rows=500000 -Dscalar.bytes=32 -Dscalar.unicode=true -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

The previous checkout copies only the same benchmark source and adds
`-Dscalar.native.expected=false`. SPLIT is absent from released Flink 1.18.1; standalone cast
parity is checked on both supported lines, with NativeCalc consumption required for successful
runtime queries and ordinary fallback retained at unverified collection boundaries.

These timings and CPU profiles precede the JVM bridge/type extension in [#469](https://github.com/datafusion-contrib/StreamFusion/pull/469). The rebased change passes 250 focused checks on Flink 2.2.1 and 213 passes plus 37 host-function skips on 1.18.1, including a temporary numeric UDF named SPLIT. The historical timings were not repeated after rebasing.



## Collection character casts {#collection-character-casts}

Standalone ARRAY/MAP CAST and TRY_CAST to STRING, VARCHAR and CHAR now use the existing
generated collection evaluator; ROW-to-character casts already used whole-Calc JVM evaluation.
This is coverage under the accepted collection-extension tradeoff. These standalone jobs are
slower than stock Flink and previous-main fallback; no acceleration is claimed.

The 2026-10-23 release/mimalloc comparison uses Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each engine processes 500,000 runtime-source rows,
parallelism 1, two warmups and three alternating trials with an 8 GiB test heap. Complete-job
timing retains planning, submission, source, rowwise blackhole sink and both row/Arrow transposes;
cluster setup and initial explain are outside timing. No other tests or benchmarks run concurrently.
Previous main `8eb2b004` uses the identical benchmark source and released native DSO in a separate
JVM, with fallback required for the new expressions. Source-matched identity controls still require
native Calc. Subsecond complete-job results include startup and do not establish sustained throughput.

DECIMAL arrays have three elements: two DECIMAL(38,9) values and a NULL. Text arrays have a
32-byte-budget Unicode string, NULL and `tail`; maps have three ordered STRING entries, including
a NULL value. Every eighth container is NULL. Trial values, variability, both paired stock controls,
identity controls and CPU counts are retained in the
[measurement summary](results/collection-character-casts-2026-10-23.json).

Median seconds (minimum–maximum):

| Expression | Stock Flink | Generated collection | Previous fallback |
| --- | ---: | ---: | ---: |
| DECIMAL ARRAY → STRING | 0.406602 (0.397808–0.410599) | 0.547139 (0.546228–0.568279) | 0.401366 (0.395287–0.406225) |
| STRING ARRAY → CHAR(32), TRY_CAST | 0.355477 (0.352671–0.357062) | 0.577292 (0.573435–0.592959) | 0.355399 (0.338167–0.361236) |
| STRING MAP → STRING | 0.561181 (0.553266–0.564460) | 0.899699 (0.887600–0.900220) | 0.559865 (0.557814–0.569516) |

Optimization was limited to two eight-second CPU captures, one each for ARRAY and MAP formatting.
The ARRAY capture has 15,151 samples across all JVM threads, with 3,660 inclusive samples in the
JVM callback and 2,521 in generated row evaluation. The MAP capture has 13,381 total samples, with
3,479 in the callback, 2,278 in generated evaluation and 1,247 in represented-string writing.
Counts overlap and include compilation/planning; they do not measure allocation or copied bytes.
Formatting, materializing strings and preserving Java caches remain visible costs. No obvious
safe shortcut removed those costs within this increment; further work stays in
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=ARRAY_FORMAT_STRING,TEXT_ARRAY_FORMAT_CHAR,MAP_FORMAT_STRING \
  -Dscalar.rows=500000 -Dscalar.bytes=32 -Dscalar.unicode=true -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

The previous checkout copies the same benchmark source and adds `-Dscalar.native.expected=false`.
For each of ARRAY_FORMAT_STRING and MAP_FORMAT_STRING, select `#profileFunction`, set
`SF_PROFILE_SCALAR=true`, `-Dscalar.warmup=1 -Dscalar.seconds=8` and
`-Dprofile.asprof=/path/to/asprof -Dprofile.outputDir=/path/to/output`. Runtime parity on both
supported Flink lines independently requires consumed NativeCalc input, including bounded UTF-16
results, legacy formatting and all four changelog kinds. NOT NULL fixed BINARY MAP leaves remain
guarded on 1.18; the [2.2 admission](#not-null-fixed-binary-map-leaves) has separate measurements.
Unverified state/native-writer boundaries retain fallback. Fixed-BINARY MULTISET elements use the
[separate verified boundary](#fixed-binary-multiset-elements).



## Fixed BINARY map values {#fixed-binary-map-values}

Nullable BINARY(n) map values now enter the existing generated collection evaluator. A direct
final MAP_VALUES of an input-reference map with VARCHAR keys reuses the Rust shared-child view;
other consumers retain Flink evaluation. NOT NULL fixed BINARY keys and values remain guarded on
1.18; the [2.2 admission](#not-null-fixed-binary-map-leaves) has separate measurements. Nullable
fixed BINARY keys are covered below. NOT NULL values have an observed NULL-formatting difference
on released Flink 1.18.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each job has 500,000 runtime-source rows, parallelism 1,
two warmups and three alternating trials per engine, with an 8 GiB test heap. The three-entry
map has fixed ASCII STRING keys and two BINARY values of the selected width plus a NULL value;
every eighth parent is NULL. Timing includes planning, submission, processing, both row/Arrow
transposes and the blackhole row sink; cluster setup and initial explain are outside timing.
No other tests, benchmarks or native compilation run concurrently with these measurements.

Previous main `01b24b62` uses identical benchmark code and the same native DSO in a separate JVM.
The new expressions must fall back there; source-matched identity controls still require NativeCalc.
The initial generated route precedes the bounded optimization; the final route uses Rust for the
direct MAP_VALUES projection. MAP-to-STRING continues to use generated Flink evaluation.
All trial values, both paired stock controls, identity controls and CPU/allocation diagnostics are
in the [measurement summary](results/fixed-binary-map-values-2026-10-24.json).

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Expression / bytes per value | Paired stock, final run | Previous fallback | Initial generated route | Final native route |
| --- | ---: | ---: | ---: | ---: |
| MAP_VALUES / 4 | 0.312171 (0.310226–0.327523) | 0.291862 (0.289075–0.299226) | 0.581776 (0.561526–0.597252) | 0.366429 (0.352071–0.367083) |
| MAP → STRING / 4 | 0.462423 (0.450366–0.471487) | 0.438994 (0.437611–0.451575) | 0.737274 (0.728585–0.741667) | 0.756175 (0.752077–0.835100) |
| MAP_VALUES / 256 | 0.400533 (0.377027–0.426528) | 0.387280 (0.383559–0.407149) | 0.685394 (0.681593–0.699388) | 0.448989 (0.446273–0.480963) |
| MAP → STRING / 256 | 1.167681 (1.161664–1.181050) | 1.173208 (1.138053–1.187564) | 1.651487 (1.616399–1.669336) | 1.763396 (1.744110–1.808813) |

The Rust view reduces MAP_VALUES native medians by 34.5–37.0% versus the generated route. Every final
standalone case remains slower than stock Flink and previous fallback, under the existing accepted
collection-extension tradeoff. These subsecond/second complete jobs do not establish sustained
throughput, and this is not a standalone acceleration claim.

Optimization stopped after two passes. An eight-second CPU capture of generated MAP_VALUES has
12,125 samples, with 2,297 inclusive samples in the JVM callback and 1,122 in generated evaluation.
The bounded change selects the existing Rust child view and avoids that callback for the verified
projection. The second eight-second capture, for MAP-to-STRING at 256 bytes, has 12,044 samples,
including 4,961 in the callback, 2,686 in generated evaluation and 2,056 in represented-string writing.
Inclusive counts overlap and include compilation/planning; they do not measure allocations or copied
bytes. Formatting, byte/string materialization and both perimeter conversions remain costs for
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Six release Criterion smokes independently assert shared child identity, offsets and parent validity
on sliced maps at 16/1024 rows and 0/4/256-byte payloads with NULL/empty parents and NULL values.
The untimed System-allocation probe makes 104 allocation requests for 6,481 bytes per case and
reports zero newly owned Arrow output-buffer bytes. This includes first schema binding and is
not a steady-state allocation or copied-byte count. The complete output buffers share input storage;
the SQL benchmarks above still retain all perimeter conversion costs.

Reproduce each width with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=FIXED_BINARY_MAP_VALUES,FIXED_BINARY_MAP_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=256 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.binary.width=4`. The previous checkout copies the same benchmark and input
fixture sources and adds `-Dscalar.native.expected=false`. For CPU captures, select
`#profileFunction`, set `SF_PROFILE_SCALAR=true`, choose one function, and use
`-Dscalar.warmup=1 -Dscalar.seconds=8 -Dprofile.asprof=/path/to/asprof`.
Rust fixtures run with `cargo bench -p streamfusion --bench collection_expressions --features mimalloc
--locked -j2 -- binary_map_values --test` from `native/`.


## Fixed BINARY map keys {#fixed-binary-map-keys}

Nullable BINARY(n) map keys use the existing length-delimited generated collection bridge.
A direct final MAP_KEYS of an input-reference map with nullable basic values reuses the Rust
shared-child view; other consumers retain generated Flink evaluation. NOT NULL fixed BINARY
keys and values remain guarded on 1.18; see the [2.2 measurements](#not-null-fixed-binary-map-leaves).
External STRUCTURED inputs and unverified state/native-writer boundaries remain guarded. Released Flink 1.18 cannot initialize CASE returning BINARY arrays; tests retain
its exact compiler failure and planning fallback for that form.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each job has 500,000 runtime-source rows, parallelism 1,
an 8 GiB test heap, two warmups and three alternating trials per engine. The ordered internal
RowData source supplies three-entry maps: two binary keys of width4/256 and a NULL key, with
INT values 1/NULL/-1. Every eighth parent is NULL. Public InternalTypeInfo registration avoids
an external byte-array-key conversion that could reorder entries by object identity.
Timing includes planning, submission, both row/Arrow transposes and a blackhole row sink;
cluster setup and initial explain are outside timing. No other tests, benchmarks or native
compilation run concurrently with these measurements.

Previous main `dbb9cffd` uses identical benchmark/input code and the same release native DSO
in a separate JVM. New expressions must fall back there, while source-matched identity controls
still require NativeCalc. The initial generated route precedes the bounded optimization;
MAP-to-STRING remains generated throughout. [All trials and diagnostics](results/fixed-binary-map-keys-2026-10-24.json)
retain both stock controls, identity controls, allocation probes and CPU profile summaries.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Expression / bytes per key | Paired stock, final run | Previous fallback | Initial generated route | Final native route |
| --- | ---: | ---: | ---: | ---: |
| MAP_KEYS / 4 | 0.243699 (0.234586–0.247416) | 0.224914 (0.224026–0.225689) | 0.383493 (0.378825–0.386779) | 0.253453 (0.252776–0.257386) |
| MAP → STRING / 4 | 0.340207 (0.336850–0.350045) | 0.339059 (0.338770–0.354235) | 0.487764 (0.482822–0.492686) | 0.482662 (0.478601–0.498011) |
| MAP_KEYS / 256 | 0.274349 (0.263257–0.276442) | 0.275038 (0.265149–0.280884) | 0.490600 (0.485984–0.510844) | 0.304727 (0.298132–0.439454) |
| MAP → STRING / 256 | 0.982406 (0.978934–0.987015) | 0.968631 (0.958710–0.969560) | 1.081767 (1.076390–1.102349) | 1.070686 (1.040013–1.078272) |

The Rust view reduces MAP_KEYS native medians by 33.9–37.9% versus generated evaluation.
All final standalone cases remain slower than paired stock and previous fallback under the
accepted collection-extension tradeoff. The wide-key native range also contains a slower trial.
These short complete jobs do not establish sustained throughput or standalone acceleration.

Optimization stopped after two passes. The first eight-second CPU capture of generated
MAP_KEYS has 14,036 samples, including 2,494 inclusive callback samples, 1,038 in generated
evaluation and 822 in array writing. Reusing the existing Rust view avoids that callback for
the verified projection. The second eight-second capture, for MAP-to-STRING at 256 bytes,
has 11,930 samples, including 4,263 in the callback, 2,907 in generated evaluation and 898 in
represented-string writing. Inclusive counts overlap and include planning and compilation;
they do not measure allocations or copied bytes. Formatting, map serialization and both
perimeter conversions remain costs for [#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Six release Criterion smokes assert shared key-child identity, offsets and parent validity on
sliced maps at 16/1024 rows and 0/4/256-byte keys with duplicate key contents, NULL values and NULL/empty parents.
The untimed System-allocation probe reports 104 allocation requests for 6,475 bytes per case
and zero newly owned Arrow output-buffer bytes, including first schema binding. Buffer sharing
and allocation requests do not measure copied bytes or end-to-end speedup.

Reproduce each width with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=FIXED_BINARY_MAP_KEYS,BINARY_KEY_MAP_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=256 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.binary.width=4`. The previous checkout copies the same benchmark/input
sources and adds `-Dscalar.native.expected=false`. For the two CPU captures, select
`#profileFunction`, set `SF_PROFILE_SCALAR=true`, choose one function, and use
`-Dscalar.warmup=1 -Dscalar.seconds=8 -Dprofile.asprof=/path/to/asprof`.
From `native/`, run `cargo bench -p streamfusion --bench collection_expressions --features mimalloc
--locked -j2 -- binary_map_keys --test` for the Rust fixtures.


## MULTISET casts {#multiset-casts}

Standalone MULTISET-to-MULTISET and MULTISET-to-STRING/VARCHAR/CHAR CAST and TRY_CAST use the
existing generated collection evaluator and map-shaped Arrow transport. Nullable elements,
counts, nested ROW/ARRAY elements, DECIMAL conversion, legacy formatting and whole-container
TRY_CAST failures retain released Flink semantics. Direct fixed BINARY elements, external
STRUCTURED inputs and unverified representation boundaries retain fallback. COLLECT and
ARRAY_AGG are separate remaining coverage; updating retained MAP/MULTISET payload work in
[#437](https://github.com/datafusion-contrib/StreamFusion/issues/437) stays deferred.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each complete job has 250,000 runtime-source rows,
parallelism 1, an 8 GiB test heap, two warmups and three alternating trials per engine. Inputs
have 4/32 entries: INT keys `i*257` with counts `i%3+1`, followed by a NULL key with count 3.
Every eighth parent is NULL. An ordered internal GenericMapData source shares the prepared,
immutable key/count map across its rows and registers it using public InternalTypeInfo.

Timing includes planning, submission, source, both row/Arrow transposes and a blackhole row sink;
cluster setup and initial explain are outside timing. Measurements run without concurrent tests,
native compilation or other benchmarks. The previous-code checkout is the PR #476 tree measured
at `d48fba98`, with identical benchmark/input code and the same native DSO. Both new expressions
must fall back there; source-matched identity controls still require a native plan.
The [full trial summary](results/multiset-casts-2026-10-24.json) retains both stock controls,
identity controls and CPU diagnostics. CSV `multiset_entries` records the selected cardinality;
text-fixture defaults such as `payload_bytes=264` are unused for this input.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Expression / entries | Paired stock | Previous fallback | Final native route |
| --- | ---: | ---: | ---: |
| INT → BIGINT elements / 4 | 0.185376 (0.182766–0.187421) | 0.191836 (0.188395–0.199897) | 0.253451 (0.246239–0.261849) |
| MULTISET → STRING / 4 | 0.226101 (0.219468–0.252986) | 0.224224 (0.216410–0.229614) | 0.287859 (0.286316–0.290994) |
| INT → BIGINT elements / 32 | 0.466856 (0.462920–0.470641) | 0.456071 (0.454004–0.466342) | 0.756280 (0.752158–0.756718) |
| MULTISET → STRING / 32 | 0.491673 (0.479175–0.542994) | 0.493186 (0.480225–0.510621) | 0.718291 (0.713210–0.727207) |

All final native cases are slower than stock and previous fallback under the accepted collection
tradeoff. These short complete jobs do not establish sustained throughput or standalone acceleration.
No new performance-only opt-in is added.

Optimization stops after two profiling/optimization passes. The eight-second numeric-cast CPU
capture has 12,872 samples, including 3,793 inclusive callback samples, 2,209 in generated evaluation,
2,812 in map serialization and 2,277 in Arrow map writing. The second eight-second capture, for
formatting, has 15,115 samples, including 4,007 in the callback, 3,303 in generated evaluation, 1,907
in map serialization and 908 in Arrow map writing. Inclusive counts overlap and include planning
and compilation; they do not measure allocation counts or copied bytes. No safe small shortcut
was identified that preserves released cast/copy semantics. Further serialization, formatting
and conversion work stays in [#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).
No production Rust kernel or JNI ownership protocol changes in this increment.

Reproduce both cardinalities with a release native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=MULTISET_CAST,MULTISET_FORMAT_STRING \
  -Dscalar.rows=250000 -Dscalar.multiset.entries=32 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.multiset.entries=4`. The previous checkout copies the same benchmark
and input sources and adds `-Dscalar.native.expected=false`. For each of the two CPU captures,
select `#profileFunction`, set `SF_PROFILE_SCALAR=true`, choose one function, and use
`-Dscalar.warmup=1 -Dscalar.seconds=8 -Dprofile.asprof=/path/to/asprof`.


## Fixed BINARY MULTISET elements {#fixed-binary-multiset-elements}

The [verified generated boundary](../operators/calc-filter.md#fixed-binary-multiset-elements)
admits nullable fixed-BINARY elements on both released lines and NOT NULL elements on 2.2.
Flink 1.18 NOT NULL elements retain the observed unchecked-NULL getter/formatting fallback.
Fixed-binary MULTISET CASE retains released implicit branch casts, including 1.18 padding.
No state, ownership or Rust operation changes. The local matrix passes 235 checks on 2.2.1
and 191 with 44 version skips on 1.18.1. This combines the broad eight-class matrix and final
24-case multiset reruns after CASE routing and deterministic-control corrections; the other
211 checks are unchanged. Benchmark smokes are excluded from correctness totals.

Release/mimalloc measurements on the Intel Core i7-12650H Linux x86-64/WSL2 host use
Flink 2.2.1, OpenJDK 17.0.20.1, parallelism one and an 8 GiB heap. Runtime internal RowData
contains an ordered four-entry multiset with 4-byte keys at 500,000 rows, then 16 entries
with 256-byte keys at 200,000 rows. Each has one NULL key with count 3; other key counts cycle
through 1/2/3. Every eighth parent is NULL. CAST changes BINARY(width) elements to
VARBINARY(width); the second query formats the original multiset as STRING.

Timing includes planning, submission, runtime source, key conversions, output, both
row/Arrow transposes and the blackhole row sink. Fresh per-job metrics-cluster/schema setup
and plan assertions are outside timing. Each admitted job requires positive consumed Calc
and both transpose counters; stock/prior-fallback jobs require no native operators. Two
warmups precede three alternating trials per engine in each separate cohort JVM. Prior
candidate 7391128b uses identical benchmark/input sources and the same frozen release DSO.
Source-matched identity controls remain native on both checkouts and are retained without
subtraction. Their variability and all independent prior stock controls remain in the
[full evidence](results/fixed-binary-multisets-2026-10-24.json).

A two-second sampler observed no overlapping external Java/Cargo workloads. Two matrix
observations and one profile observation are own Maven launch shells, identified by their
unique CSV/profile-output arguments within 0.1 seconds of phase launch before JVM replacement.
The committed summary retains those classifications; raw local reports retain every observation.

Median seconds (minimum–maximum):

| Expression | Bytes per key / entries | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| CAST | 4/4 | 0.211763 (0.204312–0.309692) | 0.205085 (0.200423–0.211794) | 0.422545 (0.412186–0.430073) |
| STRING | 4/4 | 0.337583 (0.331511–0.347192) | 0.357177 (0.339805–0.377967) | 0.490561 (0.481319–0.500198) |
| CAST | 256/16 | 0.382414 (0.379815–0.387833) | 0.379241 (0.377375–0.386449) | 1.460521 (1.444518–1.474352) |
| STRING | 256/16 | 2.529796 (2.512200–2.538575) | 2.769301 (2.761990–2.785375) | 4.928001 (4.909214–5.198685) |

All four native medians exceed both paired stock and prior fallback. These complete-job
costs ship under the accepted collection tradeoff; they establish no acceleration claim.
The shorter jobs retain startup and trial variation rather than implying sustained throughput.

The bounded investigation closes after two eight-second target CPU captures, after one
unprofiled native job each. Both use 200,000 rows, 256-byte keys and 16 entries. CAST has 11,711
samples: 2,929 inclusive in the callback, 822 in generated evaluation, 3,957 in Arrow map
writing and 2,088 in map serialization. STRING has 13,087 samples: 7,623 in the callback,
2,563 in evaluation,895 in map writing and 4,830 in represented-string writing. UTF16-to-UTF8
encoding contributes 1,595 self samples, with 527 in UTF8-to-UTF16 decoding. Counts overlap
and include fresh-cluster setup, planning, compilation and all JVM threads. They are not
allocation or copied-byte counts, nor a causal isolation of the entire slowdown against stock.
Map serialization/writing, callback conversions and represented string encoding remain costs.
No safe small shortcut preserving released key conversion and representation was found in
the two passes; further optimization belongs to
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce the small shape after a release build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=BINARY_MULTISET_CAST,BINARY_MULTISET_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=4 -Dscalar.multiset.entries=4 \
  -Dscalar.nullEvery=8 -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run rows 200000, width 256, entries 16. The prior checkout copies the same benchmark/input
sources and adds `-Dscalar.native.expected=false`. For each of the two profiles, select
`#profileFunction`, set `SF_BENCHMARK=true SF_PROFILE_SCALAR=true`, choose one function,
and retain the wide shape with `-Dscalar.warmup=1 -Dscalar.seconds=8` plus an async-profiler
path through `profile.asprof`. Profiled timings do not enter the comparison table.


## NOT NULL fixed BINARY map leaves {#not-null-fixed-binary-map-leaves}

On Flink 2.2, fixed BINARY(n) NOT NULL map keys and values now enter the existing generated
collection evaluator. The Rust MAP_KEYS/MAP_VALUES child-view whitelist is unchanged: these
new NOT NULL-element projections retain generated evaluation. Flink 1.18 keeps its fallback
because unchecked NULL getters can change released map formatting. No native operation,
transport schema, state serializer or ownership protocol changes.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Every job has 500,000 rows, parallelism one, an 8 GiB
heap, two warmups and three alternating trials per engine. Widths are four and 256 bytes.
The MAP_VALUES fixture has three ASCII STRING keys and three non-null binary values;
the MAP_KEYS fixture has three non-null binary keys with INT values 1/NULL/-1. Every eighth
parent is NULL. Performance inputs respect declared widths and nullability; separate correctness
checks include real NULL leaves and out-of-declaration lengths in both generic and binary maps.

Previous candidate `25fe176a` uses identical benchmark/input sources and the same frozen release
DSO. The current parent's later merge adds only a unit-test fixture repair and suite documentation;
production and measured benchmark sources are unchanged. Each admitted measured job requires
consumed NativeCalc and both row/Arrow transpose counters. Stock and previous-fallback jobs
require no native operators. Same-source identity controls still consume native work on both
checkouts and are retained without subtraction. Timing includes planning/submission, processing,
all intrinsic conversions and the blackhole row sink. Fresh per-job metrics-cluster/schema setup
and plan assertions are outside timing. No other local Java/Cargo work overlaps measurements.
[All trials, both stock controls, identities and profiles](results/not-null-fixed-binary-maps-2026-10-24.json)
retain the unfavorable results.

Median seconds (minimum–maximum), including complete-job startup:

| Expression / bytes per leaf | Paired stock | Previous fallback | Generated native route |
| --- | ---: | ---: | ---: |
| NOT_NULL_BINARY_MAP_VALUES / 4 | 0.295370 (0.288900–0.307176) | 0.301189 (0.289556–0.302601) | 0.620183 (0.619322–0.622486) |
| NOT_NULL_BINARY_MAP_STRING / 4 | 0.458551 (0.455308–0.476524) | 0.454731 (0.442781–0.461344) | 0.796819 (0.796102–0.801216) |
| NOT_NULL_BINARY_MAP_KEYS / 4 | 0.254220 (0.253209–0.262033) | 0.242558 (0.240891–0.245612) | 0.463470 (0.462984–0.464247) |
| NOT_NULL_BINARY_KEYS_STRING / 4 | 0.402373 (0.397256–0.413330) | 0.382396 (0.380785–0.383945) | 0.570521 (0.569917–0.572501) |
| NOT_NULL_BINARY_MAP_VALUES / 256 | 0.381256 (0.375123–0.382389) | 0.379787 (0.377312–0.388174) | 0.846614 (0.836081–0.851432) |
| NOT_NULL_BINARY_MAP_STRING / 256 | 1.484237 (1.480179–1.501203) | 1.464445 (1.449250–1.486186) | 3.074439 (3.047926–3.095101) |
| NOT_NULL_BINARY_MAP_KEYS / 256 | 0.319906 (0.318649–0.339938) | 0.310305 (0.305981–0.321606) | 0.675674 (0.658897–0.679109) |
| NOT_NULL_BINARY_KEYS_STRING / 256 | 1.383292 (1.335081–1.385634) | 1.379093 (1.372284–1.394037) | 2.419619 (2.377253–2.434042) |

Optimization stopped after exactly two eight-second CPU captures at 256-byte width. MAP_VALUES
has 13,987 samples across all JVM threads, including 2,494 inclusive callback samples, 1,120
in generated row evaluation, 1,033 in map serialization and 350 in array serialization.
Binary-key map formatting has 14,113 samples, including 6,115 in the callback, 2,426 in generated
evaluation and 3,329 in represented-string writing. Both retain copying/materialization and
perimeter costs. Counts overlap and include fresh-cluster setup, planning and compilation;
they are not allocation or copied-byte measurements and do not isolate the whole slowdown.
No safe small shortcut preserving released NULL and representation behavior was identified
inside the timebox. No further profiling was performed for this increment.

Every new native median exceeds paired stock and previous fallback. These short complete jobs
ship under the accepted collection-coverage tradeoff and do not establish standalone acceleration
or sustained throughput. Further performance work stays in
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Reproduce each width with a released native build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' -Dsf.testForks=1 \
  -Dscalar.functions=NOT_NULL_BINARY_MAP_VALUES,NOT_NULL_BINARY_MAP_STRING,NOT_NULL_BINARY_MAP_KEYS,NOT_NULL_BINARY_KEYS_STRING \
  -Dscalar.rows=500000 -Dscalar.binary.width=256 -Dscalar.nullEvery=8 \
  -Dscalar.warmup=2 -Dscalar.runs=3
```

Also run with `-Dscalar.binary.width=4`. The previous checkout copies both benchmark/input
sources and adds `-Dscalar.native.expected=false`. For each of NOT_NULL_BINARY_MAP_VALUES and
NOT_NULL_BINARY_KEYS_STRING, select `#profileFunction`, set `SF_PROFILE_SCALAR=true`, and use
`-Dscalar.native=true -Dscalar.warmup=1 -Dscalar.seconds=8`, with
`-Dprofile.asprof=/path/to/asprof -Dprofile.outputDir=/path/to/output`.


## Runtime ARRAY_SORT options (2026-10-27) {#runtime-array_sort-options-2026-10-27}

The runtime Boolean-option subset now executes in Rust for nullable integer and
DECIMAL arrays already proven shorter than 32 elements or produced by native
ARRAY_DISTINCT. Constant non-NULL options keep the existing DataFusion sort.
Unrestricted long duplicate arrays, representation-sensitive types and fallible
operands retain generated Flink evaluation. ARRAY_SORT is absent from 1.18.1.
This is one increment of [#505](https://github.com/datafusion-contrib/StreamFusion/issues/505).

The small-function comparison uses Flink 2.2.1, HotSpot 17.0.12, Rust 1.94.0
release/mimalloc without RocksDB, macOS 26.5 arm64, an Apple M4 Pro with 14 logical
cores and 24 GiB RAM. The JVM heap is 1 GiB; execution is serial, without CPU
pinning. No task builds or other tests overlap measurements; desktop activity is
uncontrolled. Baseline `eca8f717` precedes this change. The subsequently merged
integer SPLIT_INDEX increment does not change the measured ARRAY_SORT paths.

Every engine evaluates the same query and logical inputs:

```sql
SELECT ARRAY_SORT(ARRAY_SLICE(a,1,31),ascending_order,null_first) FROM inputs
```

Stock starts on prepared GenericRowData or BinaryRowData in Java. Previous
StreamFusion and the candidate start on prepared Arrow RecordBatch input in
Rust. Setup and initial expression compilation are outside timers. Stock times
released generated evaluation and complete typed-array consumption. Both native
timers run inside Rust around production compiled Calc evaluation and equivalent
typed Arrow consumption. Previous StreamFusion retains its whole-Calc kind-38
JVM callback, including the callback's Arrow import/export and conversions.
The candidate uses the production ARRAY_SLICE and typed ARRAY_SORT kernels,
without a JVM callback. Initial native entry, final output export and perimeter
row/Arrow transposes are outside this small-function boundary only.

Arrays contain repeated exact values; BIGINT values exceed 2^53 and decimals are
DECIMAL(38,9). Nullable profiles include NULL parents, elements and independently
NULL option flags. Ascending and NULL-order flags vary by row. Output schema and
complete arrays match the previous generated evaluator before timing; every
stock/previous/native trial also checks matching consumed-value checksums.
Each profile uses eight warmups and nine measured trials, rotating engine order
per trial. Batches of 1,024 rows run 32 iterations; batches of 16 run 1,024.

Median nanoseconds per row (minimum-maximum); the binary/generic column describes
stock's prepared input, and every StreamFusion input is already Arrow:

| Type / width / batch rows / stock input / NULL profile | Stock Flink | Previous StreamFusion | Rust runtime options | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| INT / 8 / 1024 / generic / nullable | 92.65 (90.81-143.65) | 247.91 (240.87-320.22) | 30.79 (29.15-33.61) | 3.01x |
| INT / 31 / 1024 / generic / nullable | 469.91 (457.15-476.93) | 781.34 (735.84-813.48) | 93.28 (90.74-98.95) | 5.04x |
| BIGINT / 8 / 1024 / binary / nullable | 139.24 (126.27-270.01) | 276.10 (255.95-325.14) | 32.58 (29.15-35.66) | 4.27x |
| BIGINT / 31 / 1024 / binary / non-null | 817.17 (799.83-858.47) | 1178.60 (1113.14-1253.36) | 82.44 (76.79-92.76) | 9.91x |
| DECIMAL / 8 / 1024 / binary / nullable | 254.94 (243.62-312.33) | 640.27 (609.27-668.54) | 33.67 (32.30-37.39) | 7.57x |
| DECIMAL / 31 / 1024 / generic / non-null | 1180.24 (1163.51-1233.21) | 2983.79 (2839.01-3053.41) | 127.77 (124.37-133.96) | 9.24x |
| INT / 8 / 16 / binary / nullable | 125.21 (121.63-133.57) | 1647.62 (1455.35-1853.17) | 93.03 (84.16-102.95) | 1.35x |
| DECIMAL / 31 / 16 / generic / nullable | 633.60 (624.04-659.60) | 3119.82 (3051.92-3340.86) | 160.04 (152.57-165.47) | 3.96x |

All eight measured medians improve on stock and previous StreamFusion. The
previous/native ratios range from 8.05x to 23.35x. These measurements establish
the applicable small-function gate; they do not measure complete-job throughput.
[All 408 trials, configuration and fingerprints](results/runtime-array-sort-2026-10-27.json)
retain warmups, ranges and the slower observations.

A separate conversion-inclusive diagnostic uses 500,000 nullable INT rows of
width eight, parallelism one, a 2 GiB heap, one warmup and three measured trials
in a fresh JVM per engine. It retains a row source, both row/Arrow transposes and
a blackhole row sink. Native plans require consumed NativeCalc work; the
candidate must have no bound JVM expression, while the previous planner must
retain its generated evaluator. Planning/submission and execution are timed;
MiniCluster/environment construction and EXPLAIN checks are outside timing.
The same release library and fixture run stock, previous, then candidate.

| Complete job | Median seconds | Minimum-maximum |
| --- | ---: | ---: |
| Stock Flink | 0.224650 | 0.219570-0.225222 |
| Previous StreamFusion | 0.277142 | 0.274509-0.286544 |
| Rust runtime options | 0.197072 | 0.193343-0.204127 |

The candidate takes 12.3% less time than stock and 28.9% less than previous in
this short job. This single profile does not establish sustained performance
for every width/type, nor remove general row/Arrow perimeter costs.

The 27 focused Flink 2.2 SQL tests pass, including positive consumed native-work
checks for all five admitted element types, independent nullable flags, a
66-element distinct array, filtering, empty input and lazy failing branches.
The unchanged released ARRAY_SORT specification also passes all 56 invocations
under stock and the injected native planner. Its 54 executing queries retain
their expected custom-type-inference fallback, with zero native-work credit;
the other two are validation cases. Native sort evidence comes from the focused
SQL contracts and the compiled-Calc test that runs without a JVM. On 1.18,
ARRAY_SORT is N/A; shared array search/distinct regressions pass on both lines.

After normal Java test compilation, construct a test classpath containing
`streamfusion-runtime/target/test-classes`, its production classes and released
dependencies, and supply it as `SF_NATIVE_BENCH_CLASSPATH`. Set
`SF_NATIVE_LIB_DIR` to the matching release library directory and select JDK 17
with `JAVA_HOME`. Run one small profile from the repository root:

```sh
cargo +1.94.0 run --manifest-path native/Cargo.toml --release --no-default-features \
  -p streamfusion --example runtime_array_sort -- int 8 true false 1024 32 8 9
```

Arguments are element (`int`, `bigint`, `decimal`), width, nullable, binary stock
input, batch rows, iterations, warmups and measured trials. The complete-job
diagnostic is optional and excluded from ordinary test runs:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=RuntimeArraySortPipelineBenchmark -Dsf.testForks=1 \
  -Darray-sort.pipeline.engine=native -Darray-sort.pipeline.rows=500000 \
  -Darray-sort.pipeline.warmups=1 -Darray-sort.pipeline.runs=3
```

Use `engine=flink` for stock. For `engine=previous`, compile the same fixture
against the preceding planner and require its bound generated ARRAY_SORT route;
the candidate planner deliberately fails that previous-route assertion.


## External STRUCTURED collection inputs {#external-structured-collection-inputs}

Source STRUCTURED values now reuse the existing generated collection evaluator and represented
ROW boundary, including nested array/map/multiset containers and ROW/STRUCTURED casts. NativeCalc
consumption establishes this route; these Java casts are not described as Rust kernels.
[Coverage](../operators/calc-filter.md#external-structured-collection-inputs) gives the exact
built-in-program, callback, state/order and writer limits.

The 2026-10-28 release/mimalloc comparison uses Flink 2.2.1/1.18.1, OpenJDK 17.0.20.1, Linux
x86-64/WSL2, parallelism one and an 8 GiB heap. Every job has 50,000 rowwise source records and a
rowwise blackhole sink, with two warmups and three alternating trials per engine. The source
array elements remain declared STRUCTURED with nullable INT/string fields. Widths are 3/16,
parent NULLs occur every eighth row, element NULLs every seventh position, and INT/string field
NULLs every eleventh/thirteenth row. Strings use 16 seeds plus a 32-character suffix. The single
projection constructs a ROW containing both an ARRAY<ROW<a BIGINT,b STRING>> cast and character
formatting of the original array; it measures their combined production boundary.

Candidate and actual previous `86c9d244` use identical fixture sources and the **same frozen native
release library**. The previous planner must fall back without NativeCalc. Each candidate native
job requires 50,000 consumed NativeCalc inputs and `jvm=[ROW, CAST]` descriptors. Both perimeter
transposes, JNI, generated evaluation and planning/submission remain timed; environment setup
and the initial explain probe remain outside timing. Local exchange caching stays disabled.
No competing Java/native build, test or benchmark process was observed by the process monitor.

[All 80 jobs, 20 native Calc witnesses and paired controls](results/structured-collection-inputs-2026-10-28.json)
retain source/library hashes, raw trials, commands and ranges. Median seconds (min–max):

| Flink | Array width | Previous paired stock | Previous fallback | Candidate paired stock | NativeCalc |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2 | 3 | 0.194330 (0.170898–0.262270) | 0.191025 (0.175968–0.222784) | 0.177382 (0.175108–0.182358) | 0.384402 (0.365160–0.412428) |
| 2.2 | 16 | 0.370867 (0.356476–0.390086) | 0.372421 (0.365365–0.372573) | 0.383198 (0.380443–0.386313) | 0.735325 (0.727280–0.855846) |
| 1.18 | 3 | 0.178257 (0.174778–0.197603) | 0.183170 (0.182411–0.217055) | 0.184813 (0.178814–0.220786) | 0.382043 (0.360289–0.391352) |
| 1.18 | 16 | 0.375509 (0.362542–0.478923) | 0.378216 (0.367642–0.418500) | 0.383667 (0.374440–0.396060) | 0.779504 (0.709350–0.798988) |

Every native median loses to both stock and the preceding fallback. The new route is roughly
twice paired stock on these workloads. This ships as correctness-verified collection coverage
under the existing accepted collection-extension tradeoff, without an acceleration claim or
an additional performance-only switch. Standalone optimization remains in
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

Exactly two eight-second CPU captures on Flink 2.2, one at each width, close the bounded
investigation. Each has one unprofiled native warmup followed by native-only complete jobs;
profiled timings are excluded from the table. Width 3/16 captures have 20,683/15,380 execution
samples. Row serialization appears in 349/800 samples, Arrow field writing in 476/789,
generated evaluation in 385/513 and the existing JVM batch callback in 309/374. Categories
are inclusive and overlap. They include JIT, job setup and all process threads, and do not
measure allocations, copied bytes or isolated kernel costs. No small safe change to this
existing serialization/representation boundary was selected. The report retains capture
hashes, scope and the remaining conversion/serialization costs; no third tuning pass follows.

Reproduce the complete-job comparison after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=DynamicCollectionBenchmark#compareExpressions' \
  '-Dcollection.expression=ROW(CAST(arr AS ARRAY<ROW<a BIGINT,b STRING>>),CAST(arr AS STRING))' \
  -Dcollection.map=false -Dcollection.type=STRUCTURED -Dcollection.width=3 \
  -Dcollection.rows=50000 -Dcollection.bytes=32 -Dcollection.domain=16 \
  -Dcollection.warmup=2 -Dcollection.runs=3 -Dcollection.native=true -Dsf.testForks=1
```

Repeat at width 16 and add `flink-1.18` to the profile list for the second released line. The
previous checkout copies the identical benchmark fixture, sets `collection.native=false` and
loads the same release library. For the CPU diagnostic, additionally set
`SF_PROFILE_COLLECTION=true`, select `DynamicCollectionBenchmark#profileNativeExpressions`
and set `collection.profileSeconds=12`. After its `collection-profile-ready` message, attach
`asprof -d 8 -e cpu -i 1ms -f structured-width3.jfr PID`; repeat once at width 16. The diagnostic
requires positive native work in every job and does not supply timing results for comparison.

