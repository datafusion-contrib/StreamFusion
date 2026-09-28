# Host-exact builtins over the same upcall, with a faster pure-Rust opt-in

**Applies to:** REGEXP_EXTRACT, UPPER/LOWER, temporal parsing/formatting/arithmetic/casts

Builtins whose Rust implementation can diverge from the JVM's — REGEXP_EXTRACT (regex dialects),
UPPER/LOWER (locale case folding), DATE_FORMAT/EXTRACT over `TIMESTAMP_LTZ` (time-zone database
edges) — default to calling Flink's own implementation through the same batch upcall used for user
UDFs: byte parity, island preserved.

The pure-Rust path stays available behind `allowIncompatible` (see
[Configuration](../configuration.md)) for callers who want the faster path and can accept the
divergence risk.

q21 measures the honest price of the guarantee: 0.76x via the upcall vs 1.57x pure-native. For the
zone-aware datetime functions the two paths measure within noise of each other — the call itself
isn't the bottleneck there.

Temporal expressions also use Flink's generated expression code through this bridge. Adjacent
temporal calls fuse into a single evaluator: for `DATE_FORMAT(TO_TIMESTAMP(text), pattern)`,
only the input strings and final formatted result cross the bridge. The intermediate timestamp
stays in Flink's internal representation, preserving its full range without another upcall.
This expands coverage; isolated temporal projections have not demonstrated an end-to-end speedup.
See [the temporal contract](../operators/temporal-functions.md) and
[release measurements](../benchmarks/scalar-functions.md#temporal-coverage-diagnostic-2026-09-15).

## Exact floating math: remaining performance limits

A September 27, 2026 investigation compared released Flink 2.2.1 with the exact math callback
on JDK 17, Linux x86-64/Core i7-12650H, release+mimalloc, 2 GiB heap, parallelism one and default
1,024-row batches. Measurements use 2M runtime rows, no injected NULLs, two warmups and five
alternating trials per engine, retaining the row source, sink, JNI and both transposes.
Baseline complete-job medians:

| Expression | Native seconds | Flink seconds |
| --- | ---: | ---: |
| TAN | 0.487 | 0.359 |
| COSH | 0.468 | 0.345 |
| Floating TRUNCATE | 0.952 | 0.895 |
| Source-matched identity | 0.344 | 0.285 |

A 20-second CPU profile of native TAN attributes 11.5% of samples directly to libm tangent,
18.3% inclusively to the scalar callback, and 10.9% inclusively to Arrow-to-row conversion.
The profile does not support treating libm itself as the only bottleneck. Inclusive categories
overlap and must not be added together.

Three prototypes passed targeted correctness tests but failed whole-job performance validation
and were removed:

- Borrowed Arrow inputs with a primitive DOUBLE result loop: TAN 0.482s, COSH 0.463s,
  TRUNCATE 0.960s native, versus Flink 0.348s, 0.348s and 0.901s.
- Direct primitive-vector reads in generated expressions: 0.480s, 0.471s and 0.949s native,
  versus Flink 0.354s, 0.352s and 0.917s. The small TAN movement did not establish a meaningful gain.
- Single-step decimal rounding for floating TRUNCATE: native median 0.957s (0.949–0.961),
  Flink 0.886s (0.883–0.901), identity 0.335s native / 0.279s Flink. This also failed to improve
  the previous native implementation.

Matched 20-second TRUNCATE CPU profiles for the rounding prototype and stock Flink explain the
remaining cost. BigDecimal occurs in 43.0% of native samples and 48.7% of Flink samples;
conversion from floating point into decimal alone accounts for 29.5% and 33.0%. FloatingDecimal
formatting occurs in 19.5% and 22.0%. Removing decimal-point shifts leaves the expensive conversion
in place, while native execution also pays its callback and representation boundaries. An exact
replacement must preserve the released JDK/Flink decimal conversion and exceptional results;
approximate floating rounding would change the contract.

The exact floating-math PR remains draft: correctness and these unsuccessful experiments do not
establish acceleration. Reproduce with `ScalarFunctionBenchmark#individualFunctions`,
`-Dscalar.functions=TAN_EXACT,COSH_EXACT,FLOAT_TRUNCATE_EXACT`,
`-Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5`, `SF_BENCHMARK=true`, `-Pbench`
and `-Dsf.extraJvmArgs=-Xmx2g`. The retained implementation still uses released generated math.


### Composition with grouped integer sums

A September 28 follow-up retains the exact callback and uses the generated fixed-width
exit projection described in [the transpose ledger](zero-copy-exit-transpose.md).
At 2M rows, standalone TAN still takes 0.430s native versus 0.311s Flink, COSH
0.419s versus 0.304s, and floating TRUNCATE 0.917s versus 0.857s. A fresh TAN CPU
profile attributes 18.1% of native samples inclusively to the callback and 10.5% to
exit conversion; the input operator's 40.0% includes downstream execution and cannot
be added to those figures. Reusing callback vector roots and preallocating numeric
input writes did not establish a repeatable whole-job gain and were removed.

Composing each expression with an integer SUM over 64 groups gives the native
aggregate enough work to amortize the callback and perimeter costs. These are
additional scalar-harness queries; the Nexmark harness is unchanged. Both engines
use identical mini-batch settings (1,024 rows, five-second maximum latency), a row
source and row sink. Plan assertions require NativeCalc, NativeColumnarGroupAggregate,
and both transposes. The same release+mimalloc, JDK 17, Flink 2.2.1, 2 GiB heap,
parallelism one, two warmups and five alternating trials apply. No NULLs are injected
in the timing data. Medians and observed trial ranges, in seconds:

| Query | Rows | Native median (range) | Flink median (range) |
| --- | ---: | ---: | ---: |
| TAN grouped SUM | 2M | 0.586 (0.582–0.616) | 0.734 (0.721–0.891) |
| COSH grouped SUM | 2M | 0.573 (0.567–0.580) | 0.712 (0.678–0.720) |
| Floating TRUNCATE grouped SUM | 2M | 1.060 (1.055–1.070) | 1.259 (1.248–1.268) |
| TAN grouped SUM | 5M | 1.313 (1.310–1.349) | 1.722 (1.711–1.763) |
| COSH grouped SUM | 5M | 1.306 (1.303–1.311) | 1.631 (1.600–1.648) |
| Floating TRUNCATE grouped SUM | 5M | 2.549 (2.530–2.566) | 3.036 (2.988–3.060) |
| Numeric identity control | 2M | 0.301 (0.292–0.351) | 0.230 (0.226–0.245) |
| Numeric identity control | 5M | 0.633 (0.615–0.674) | 0.464 (0.455–0.594) |

The expressions are `SUM(CAST(f(n) * 1000 AS BIGINT))`, grouped by
`MOD(ABS(n), 64)`, where `f(n)` is `TAN(CAST(n AS DOUBLE))`,
`COSH(CAST(n % 20 AS DOUBLE))`, or
`TRUNCATE(CAST(n AS DOUBLE) / 7E0, CAST(n % 4 AS INT))`.
The integer cast avoids nondeterministic floating-point SUM reassociation.
Separate grouped parity cases cover nullable inputs and extreme values on both
Flink 1.18 and 2.2, with deterministic count-based mini-batch boundaries.

These composed workloads use 16–24% less elapsed time than stock Flink, with
nonoverlapping observed ranges. This is a pipeline result, not a standalone math
speedup. The standalone performance gap remains outstanding; the PR remains draft.
The identity controls also remain slower than Flink.

A separate 5M-row admission ablation restored the numeric cases in
`needsExactScalarFunction` to their state before commit `c01f3db2`, while keeping
all other current code and resource settings. The benchmark required the grouped
plans to have no NativeCalc, permitted fallback execution, and labeled those trials
`previous-admission` rather than native. Each plan reported zero native operators:
TAN required its incompatible flag, COSH was unsupported, and floating TRUNCATE
required DECIMAL. This isolates the previous admission behavior; it is not a build
of an entire historical release.

| Query | Previous admission median (range) | Matched Flink median (range) |
| --- | ---: | ---: |
| TAN grouped SUM | 1.710 (1.659–1.741) | 1.691 (1.636–1.736) |
| COSH grouped SUM | 1.573 (1.520–1.600) | 1.541 (1.517–1.554) |
| Floating TRUNCATE grouped SUM | 2.999 (2.976–3.015) | 3.008 (2.987–3.011) |

Current native medians are approximately 23%, 17% and 15% lower than the previous
admission medians, with disjoint trial ranges. Stock controls shifted by up to 5.5%
between runs, so those percentages are not exact causal estimates. The direction
of improvement holds against both Flink controls and the previous-admission trials.
The temporary admission and benchmark changes were removed after measurement.

[Raw measured trials](../benchmarks/exact-math-composition-2026-09-28.csv) retain
both engines and the unfavorable standalone/control results. Reproduce grouped
measurements with `ScalarFunctionBenchmark#individualFunctions`, `-Pbench`,
`SF_BENCHMARK=true`, `-Dsf.extraJvmArgs=-Xmx2g`,
`-Dscalar.functions=TAN_GROUPED_SUM,COSH_GROUPED_SUM,FLOAT_TRUNCATE_GROUPED_SUM`,
`-Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5`; repeat with
`-Dscalar.rows=5000000` for the larger run.
