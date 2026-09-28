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
