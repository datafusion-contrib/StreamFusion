# Scan basic arrays without rebuilding JVM rows

ARRAY_CONTAINS and ARRAY_POSITION already belong to the experimental collection
extension. For matching nullable BOOLEAN, integer and DECIMAL elements, their
verified signatures scan Arrow's flat values and list offsets in Rust. Type
dispatch happens once per batch; the scan does not construct an array per row.
Admitted scalar needles stay scalar rather than being expanded to the batch size.

This follows Comet's typed search structure while retaining Flink's results:
CONTAINS can match a NULL needle to a NULL element; POSITION returns nullable INT,
with the first one-based match or zero for absence. DECIMAL compares exact unscaled
integers at the resolved precision and scale. DataFusion's NULL-needle and missing
position results cannot be reused unchanged.

Flink-generated evaluation remains necessary for strings, nested values, NOT NULL
elements, mixed resolved types and fallible arguments. Non-NULL literal or
nonnullable CONTAINS needles also retain Flink's element-getter behavior. DECIMAL
arrays with those needles require complete stock-Calc fallback: the Arrow-backed
JVM view does not reproduce an unchecked binary-array read of a NULL decimal.
See [coverage and restrictions](../operators/calc-filter.md#collection-expressions).

## Avoid temporary bytes for small values of wide DECIMAL types

A declared precision above 18 does not imply that the actual unscaled value needs
more than 64 bits. Like Comet's decimal writer, the Java entry writer tests the
actual BigInteger bit length and uses Arrow's public signed-long setter when it
fits. Larger values retain the BigDecimal setter. Rescaling, precision overflow,
NULLs and signed 128-bit storage remain unchanged; there is no direct memory access
or new ownership protocol.

A JFR capture before this setter change contains 2,112 execution samples, including
355 stacks with StreamFusion frames. DecimalAccessor/Arrow's BigDecimal setter
appears in 98 inclusive stacks, and BigInteger.toByteArray in 37. Counts overlap
and include startup, warmups and both engines; they are not isolated percentages
of kernel CPU time. [Profile counters](../benchmarks/array-search-profile-2026-10-19.json)
retain the scope and recording checksum. Profiled elapsed times are excluded from
the comparisons below.

## Complete-job evidence

Measurements use an Apple M4 Pro (14 cores), macOS arm64, JDK 17.0.12, Flink 2.2.1,
release Rust with mimalloc and
the default RocksDB feature disabled for these stateless jobs. Parallelism is one
with a 2 GiB heap. A rowwise sequence source constructs arrays and nullable needles;
the sink is rowwise blackhole. Both transposes, planning, startup, JNI and conversion
remain measured. Each cohort starts a fresh JVM, performs two warmups per engine
and five alternating stock/StreamFusion trials. No task build or other test runs
overlap timing; desktop activity is uncontrolled and CPUs are not pinned.

The original control is revision `616691f5`: initial controls run before the
implementation, and later controls use its frozen planner encoder, DECIMAL setter
and native DSO with the identical fixture. The final non-DECIMAL code is unchanged
by the subsequent DECIMAL setter optimization. Arrays are NULL every eighth row,
elements every seventh slot, and needles every seventh row. Domain equals width.
For the wide-value control, both arrays and needles add 9007199254740993 before
conversion to DECIMAL(38,18), ensuring unscaled values exceed signed 64 bits.

All times below are medians in seconds. Negative percentages mean less elapsed
time. Both stock columns are retained to expose drift between separate JVMs.

| Case | Rows | Prior stock | Prior SF | Final stock | Final SF | Versus prior SF | Versus paired stock |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CONTAINS BIGINT, width 8 | 10M | 1.370958 | 2.099639 | 1.403881 | 1.381453 | -34.21% | -1.60% |
| POSITION BIGINT, width 8 | 10M | 1.381318 | 2.175370 | 1.352340 | 1.372358 | -36.91% | +1.48% |
| POSITION INT, width 64 | 2M | 0.828653 | 1.200763 | 0.819694 | 0.712874 | -40.63% | -13.03% |
| CONTAINS BOOLEAN, width 8 | 10M | 1.179445 | 1.927238 | 1.224895 | 1.345062 | -30.21% | +9.81% |
| POSITION DECIMAL, width 8 | 10M | 3.582922 | 7.481526 | 3.774973 | 4.360048 | -41.72% | +15.50% |
| POSITION wide-value DECIMAL, width 8 | 2M | 0.954803 | 2.016706 | 1.092587 | 1.426982 | -29.24% | +30.61% |

INT width 64 has nonoverlapping stock/native ranges: 0.813098-0.834755 versus
0.699956-0.722105 seconds. BIGINT short-array ranges overlap; these comparisons do
not establish a sustained gain over stock Flink. BOOLEAN and DECIMAL still lose
to stock. This improves evaluation inside the already-admitted experimental
extension; it does not establish a universally accelerated standalone collection
path. Its existing default and switch are unchanged. The previously accepted
standalone losses remain tracked by
[issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

The DECIMAL setter alone reduces the ten-million-row native median from 5.041354
to 4.360048 seconds (-13.51%), with paired stock medians 3.774383 and 3.774973.
The corresponding two-million-row reduction is 1.126859 to 0.865803 seconds
(-23.17%). For truly wide values, the old setter with Rust kernels measures
1.475323 seconds (1.459724-1.507564); the final setter measures 1.426982 and
1.454255 seconds in separate cohorts (1.396662-1.469906 combined). Ranges overlap
and stock controls move, so no independent large-value setter gain is claimed.

[All 260 measured trials](../benchmarks/array-search-trials-2026-10-19.csv),
[all cohort medians and ranges](../benchmarks/array-search-summary-2026-10-19.csv)
and the [method and library checksums](../benchmarks/array-search-method-2026-10-19.json)
retain initial kernel-only measurements, the final setter, large-value controls
and unfavorable cases. Two original constant-CONTAINS probes are unmatched
controls: they precede discovery of the getter restriction and are excluded from
the primary comparisons. Candidate jobs assert the function engine as well as
NativeCalc and both transposes.

## Reproduce and validate

Use JDK 17 and build the same stateless release configuration before timing:

```sh
cargo build --manifest-path native/Cargo.toml --release --locked \
  -p streamfusion --no-default-features --features mimalloc
mvn -pl streamfusion-runtime -am test-compile -Pbench \
  -DskipTests -Dnative.build.skip=true
SF_BENCHMARK=true mvn -pl streamfusion-runtime surefire:test -Pbench \
  -Dtest=DynamicCollectionBenchmark -Dcollection.map=false \
  -Dcollection.type=INT -Dcollection.width=64 -Dcollection.domain=64 \
  -Dcollection.rows=2000000 -Dcollection.warmup=2 -Dcollection.runs=5 \
  '-Dcollection.expression=ARRAY_POSITION(arr,idx)' \
  -Dcollection.rustFunction=ARRAY_POSITION -Dsf.extraJvmArgs=-Xmx2g
```

For DECIMAL use `collection.type=DECIMAL` and cast the needle to DECIMAL(38,18).
The wide-value fixture additionally sets
`collection.decimalOffset=9007199254740993` and uses
`ARRAY_POSITION(arr,CAST(idx + 9007199254740993 AS DECIMAL(38,18)))`.
These properties belong to the test harness, not deployment configuration.

The [Criterion suite](../benchmarks/native-criterion.md) exercises production
compiled Calc, result allocation and scalar-registry adaptation. Its 144 fixtures
cover empty/16/1,024-row batches, empty/8/64-element arrays, sliced buffers,
BOOLEAN/INT/BIGINT/DECIMAL, NULL parents/elements/needles, duplicates and misses.
[Allocation probes](../benchmarks/array-search-allocations-2026-10-19.csv) exclude
first-expression compilation. They report requested allocation and output buffer
sharing, not copied bytes or complete-job throughput.

The [validation inventory](../benchmarks/array-search-validation-2026-10-19.json)
records 672 passing Rust core tests, 212 selected Java checks on Flink 2.2/JDK 17,
and 179 passing checks with 33 released-catalog skips on Flink 1.18/JDK 11.
The 17 new search checks run on both lines without skips, including runtime
integer limits, exact large decimals, filters, NULLs, generated routes and failure
behavior. Five decimal-boundary tests include signed-long limits, adjacent wider
values, precision 38, scale changes, overflow and vector growth. All 144 Criterion
fixtures and 18 scalar-registry cases for the two new opcodes pass smoke checks.
These are the selected collection/decimal regressions, not the full JVM or
stateful recovery suite.

## Integer and decimal array extrema

ARRAY_MIN and ARRAY_MAX also avoid the generated JVM evaluator for nullable
TINYINT, SMALLINT, INT, BIGINT and DECIMAL elements. They reuse released DataFusion
54 directly: one typed dispatch per batch, a flat-values scan for lists shorter
than 32, and Arrow's exact integer/decimal reduction for longer lists. This follows
Comet's delegation for matching primitive semantics without copying another kernel
into StreamFusion. NULL elements are skipped; NULL, empty and all-NULL arrays
return a nullable result of the declared element type, including DECIMAL precision
and scale. Floating, string, temporal, nested and NOT NULL-element signatures and
fallible arguments keep the verified generated route. No configuration is added.
The [earlier Linux investigation](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/234-array-extrema.md)
rejected broader standalone admission after a sustained short-array regression.
The measurements here optimize the subsequently admitted collection extension on
macOS; they do not invalidate that earlier result or establish platform-wide gains.

The following whole-job comparison uses the same machine, resources, release
allocator, row boundaries and alternating-trial protocol as the search measurements
above. The baseline is `180d55e7`, the existing PR implementation before these
extrema registrations. All six baseline cohorts run before the implementation;
candidate cohorts assert the Rust function engine and both transposes. Each cohort
uses a fresh JVM, two warmups per engine and five measured trials per engine.
No task build or other test overlaps timing; background desktop activity remains
uncontrolled. These measurements include planning/startup rather than isolating
sustained reduction throughput.

Arrays are NULL every eighth row and elements every seventh slot. Values use
`(31 * elementIndex + row) % width - width / 2`, at widths 8 and 64. DECIMAL uses
DECIMAL(38,18); the wide-value MAX case adds 9007199254740993 before conversion,
ensuring actual unscaled values exceed signed 64 bits. The entry-writer optimization
described above is present in both implementations. Medians are seconds:

| Case | Rows | Prior stock | Prior SF | Final stock | Final SF | Versus prior SF | Versus paired stock |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MIN BIGINT, width 8 | 10M | 1.678578 | 2.145354 | 1.487713 | 1.342250 | -37.43% | -9.78% |
| MAX BIGINT, width 64 | 2M | 1.043066 | 1.583317 | 1.046647 | 1.053030 | -33.49% | +0.61% |
| MIN INT, width 64 | 2M | 1.045487 | 1.396983 | 1.034057 | 0.704146 | -49.60% | -31.90% |
| MAX INT, width 8 | 10M | 1.616078 | 2.019303 | 1.470535 | 1.204189 | -40.37% | -18.11% |
| MIN DECIMAL, width 8 | 2M | 0.910065 | 1.422525 | 0.988346 | 0.980244 | -31.09% | -0.82% |
| MAX wide-value DECIMAL, width 8 | 2M | 1.091502 | 2.165275 | 1.362615 | 1.634822 | -24.50% | +19.98% |

The two INT cases and short BIGINT case have nonoverlapping paired stock/native
ranges. Long BIGINT and small-value DECIMAL ranges overlap: they do not establish
an advantage over stock Flink. Wide-value DECIMAL still loses to paired stock,
despite improving the previous evaluator. Its stock control also shifts materially
between cohorts, so the before/after columns must remain visible. The existing
experimental extension and its previously accepted standalone limits are unchanged;
[issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392) remains
open. These trials do not isolate the remaining bottleneck. The earlier decimal
profile identifies entry costs, and truly wide values still use the BigDecimal
setter, but no new causal profile or universal acceleration claim is made here.

[All 120 measured trials](../benchmarks/array-extrema-trials-2026-10-20.csv),
[medians and ranges](../benchmarks/array-extrema-summary-2026-10-20.csv) and the
[method/library checksums](../benchmarks/array-extrema-method-2026-10-20.json)
retain all six matched comparisons. Reproduce with the command above, using
`collection.expression=ARRAY_MIN(arr)` or `ARRAY_MAX(arr)` and the corresponding
`collection.rustFunction`; fixture type, width and row count are recorded per case.
The wide-value MAX case also sets `collection.decimalOffset=9007199254740993`.

The [360 allocation probes](../benchmarks/array-extrema-allocations-2026-10-20.csv)
come from the compiled production Calc suite, covering all four integer types and
DECIMAL(38,9), 0/16/1,024 rows, widths 0/8/31/32/33/64, sliced lists and NULLs.
Expected results and fixture construction stay outside timed execution; requested
allocation and shared buffers are not copied-byte measurements. Four Rust checks
add signed limits, hidden children, separately sliced parent/child buffers, typed
empty batches, scalar adaptation, exact precision-38 decimals and scales 0/9/18/38.
SQL checks compare stock/native results and schemas, filter outcomes, all-filtered
batches and failing operands, and verify Rust/generated assignment explicitly.

The [current validation inventory](../benchmarks/array-extrema-validation-2026-10-20.json)
records 677 passing Rust core tests, 224 selected Java checks on Flink 2.2/JDK 17,
and 185 passing checks with 39 released-catalog skips on Flink 1.18/JDK 11. The 26
extrema checks pass on Flink 2.2; 13 ARRAY_MAX checks pass and 13 ARRAY_MIN checks
skip on Flink 1.18. Extrema cases check each
function independently: ARRAY_MIN is absent from Flink 1.18, while ARRAY_MAX still
receives the complete DECIMAL scale and error-path coverage. All 360 Criterion
extrema fixtures and 18 new scalar-registry cases pass smoke checks. These are
selected regressions, not the full JVM or stateful recovery suite.
