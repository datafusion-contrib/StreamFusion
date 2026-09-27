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
This expands coverage; the original generic-callback measurements did not demonstrate an
end-to-end speedup for isolated temporal projections. The exact canonical TRY_CAST subset below
is measured separately.
See [the temporal contract](../operators/temporal-functions.md) and
[release measurements](../benchmarks/scalar-functions.md#temporal-coverage-diagnostic-2026-09-15).

## Canonical timestamp TRY_CAST

Profiles of STRING-to-TIMESTAMP TRY_CAST identify Flink's general `DateTimeFormatter` field maps
as a major cost after caching Arrow timestamp writers. A standalone TRY_CAST of a direct input
now recognizes the exact ASCII `yyyy-MM-dd HH:mm:ss[.fraction]` subset, with years 1–9999 and
1–9 fractional digits. It builds a validated `LocalDateTime`, truncates the fraction to the
declared precision, and uses Flink's same `TimestampData` conversion. TIMESTAMP_LTZ uses the same
session `TimeZone` and `atZone` conversion, including DST gaps and overlaps.

This is a fast path inside the existing batch callback, so JNI and both perimeter transposes
remain. An unrecognized value continues through the original generated TRY_CAST; this preserves
Flink's optional fields, noncanonical widths, SMART date resolution, and malformed-input policy.
Only a direct input reference is read speculatively. Nested calls and complete-row evaluators
retain their original generated code and evaluation order.

Differential tests compare all precisions 0–9 in four zones over deterministic random dates,
the timestamp range limits, pre-epoch fractions, and DST transitions. SQL tests additionally
verify the fallback's NULL/error behavior, legacy configuration, child failures, and untaken
branches on both released Flink lines.

On Linux x86-64 (Core i7-12650H), JDK 17, released Flink 2.2.1 and release+mimalloc,
2M rows at parallelism one, a 2 GiB heap, the system session zone (`America/New_York`), two warmups and five alternating trials yield:

| Target | Generic callback + cached writer median (range), s | Canonical parser median (range), s | Stock Flink median (range), s | Flink/native |
| --- | ---: | ---: | ---: | ---: |
| TIMESTAMP(9) | 3.776 (3.764–3.813) | 0.855 (0.841–0.875) | 3.444 (3.402–3.458) | 4.03x |
| TIMESTAMP_LTZ(9) | 3.791 (3.779–3.865) | 0.861 (0.855–0.881) | 3.474 (3.466–3.499) | 4.03x |

The source-matched identity control is 0.680 s native / 0.411 s Flink. The benchmark alternates
`2000-02-29 12:34:56` and `1969-12-31 23:59:59`, without NULLs; both row/Arrow transposes,
JNI, the runtime row source and row sink remain included and the plan requires native Calc.
The parser reduces native time by about 77% versus the cached-writer implementation and 80%
versus the original callback (4.329/4.381 s). These gains apply to the measured canonical subset;
noncanonical parsing and Boolean TRY_CAST remain separate performance work.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=TRY_STRING_TO_TIMESTAMP,TRY_STRING_TO_TIMESTAMP_LTZ \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 -Dsf.extraJvmArgs=-Xmx2g
```

Repeating the same workload with `-Dscalar.nullEvery=7` gives TIMESTAMP 0.817 s native
(range 0.800–0.861) versus 3.002 s Flink (2.965–3.027), and TIMESTAMP_LTZ 0.830 s native
(0.817–0.848) versus 3.053 s Flink (3.049–3.080): 3.68x for both. The nullable identity
control is 0.633 s native / 0.391 s Flink. Both runs preserve the original input generator.
