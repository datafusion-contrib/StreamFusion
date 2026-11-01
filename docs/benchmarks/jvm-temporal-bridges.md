# Direct JVM temporal bridge costs

These measurements accompany the [verified table-function coverage](../operators/unnest-correlate.md#alternate-direct-temporal-bridges-2026-11-01).
The admitted paths are correctness-verified UDF coverage under the accepted performance exception.
They are slower in these standalone jobs; this is not an acceleration claim.

The [complete artifact](results/jvm-table-function-temporal-bridges-2026-11-01.json) retains all
warmup/measured trials, their order, immutable revisions, fixture/native hashes, configuration,
scoped correctness counts and sampled host observations. The candidate is `5ee08ce3`; the actual
previous production revision is `f5274a10`. Native sources are unchanged, so both revisions use
the same freshly built release/mimalloc DSO. Only new test/benchmark fixtures are copied into
the previous worktree; its production Java sources remain unchanged.

## Method and limits

Released Flink 1.18.1 and 2.2.1 use Temurin 17.0.17, one task, a 2 GiB heap and 1,024-row
transpose batches on a Darwin arm64 machine with ten logical CPUs and 16 GiB memory. Each case
uses two warmup pairs and five alternating measured pairs. Inputs contain the same values,
types, NULLs, cardinalities and container widths. SQL setup and symmetric EXPLAIN precede the
job timer; submission, callbacks, released conversion, owned writes, row source/blackhole sink
and both perimeter transposes remain inside it. Cluster creation/teardown is outside it.
Every candidate trial requires both transposes, no fallback and positive table-function input.
The previous enabled route requires zero native substitutions/work for these new signatures.

The 262,144-row matrix includes boxed DATE with ARRAY/List, all eight additional temporal
bridges with ARRAY, and JDBC DATE/TIMESTAMP with List. A separate 2,097,152-row JDBC TIMESTAMP/List
case retains less favorable sustained-processing observations as well as short jobs.
Tables show median seconds and full measured ranges; every warmup is retained in the artifact.
The two stock columns deliberately retain the independent controls rather than hiding their drift.

Background macOS indexing/analysis consumed substantial sampled CPU. No other Java process was
observed during timed series. The larger stock controls drift between JVMs and include outliers;
these runs cannot isolate the effect of a conversion optimization. Host observations also have
UTC gaps, so process wall duration is not used as throughput. Short-job startup/JIT costs and
unfavorable complete-job results are retained. No optimization or performance-only opt-in is
introduced by these coverage changes.

## Flink 2.2

| Case / source rows | Stock in candidate JVM | Candidate native | Stock in previous JVM | Previous enabled fallback |
| --- | --- | --- | --- | --- |
| boxed DATE / array / 262,144 | 0.112 (0.108–0.123) | 0.151 (0.146–0.155) | 0.110 (0.101–0.114) | 0.108 (0.104–0.114) |
| boxed DATE / list / 262,144 | 0.654 (0.647–0.664) | 1.200 (1.195–1.207) | 0.653 (0.650–0.669) | 0.660 (0.647–0.663) |
| date-sql:array / 262,144 | 0.100 (0.099–0.111) | 0.197 (0.196–0.200) | 0.097 (0.095–0.099) | 0.098 (0.094–0.107) |
| time-integer:array / 262,144 | 0.099 (0.095–0.108) | 0.190 (0.189–0.194) | 0.094 (0.093–0.103) | 0.092 (0.091–0.103) |
| time-long:array / 262,144 | 0.107 (0.097–0.114) | 0.192 (0.189–0.202) | 0.098 (0.095–0.099) | 0.096 (0.092–0.108) |
| time-sql:array / 262,144 | 0.105 (0.102–0.113) | 0.198 (0.196–0.208) | 0.099 (0.098–0.102) | 0.101 (0.097–0.108) |
| timestamp-sql:array / 262,144 | 0.114 (0.112–0.128) | 0.205 (0.203–0.208) | 0.108 (0.105–0.109) | 0.108 (0.105–0.109) |
| ltz-integer:array / 262,144 | 0.100 (0.097–0.101) | 0.202 (0.197–0.209) | 0.100 (0.096–0.102) | 0.097 (0.095–0.101) |
| ltz-long:array / 262,144 | 0.100 (0.098–0.102) | 0.201 (0.199–0.207) | 0.100 (0.097–0.107) | 0.100 (0.097–0.102) |
| ltz-sql:array / 262,144 | 0.101 (0.098–0.104) | 0.198 (0.194–0.203) | 0.097 (0.094–0.099) | 0.100 (0.096–0.101) |
| date-sql:list / 262,144 | 0.622 (0.621–0.632) | 2.026 (2.014–2.038) | 0.625 (0.623–0.627) | 0.626 (0.624–0.886) |
| timestamp-sql:list / 262,144 | 0.640 (0.632–0.671) | 2.053 (2.037–2.071) | 0.641 (0.638–0.652) | 0.636 (0.632–0.647) |
| timestamp-sql:list / 2,097,152 | 12.117 (12.103–14.893) | 23.871 (23.589–27.927) | 6.119 (6.086–6.321) | 6.142 (6.009–6.181) |

## Flink 1.18

| Case / source rows | Stock in candidate JVM | Candidate native | Stock in previous JVM | Previous enabled fallback |
| --- | --- | --- | --- | --- |
| boxed DATE / array / 262,144 | 0.115 (0.112–0.119) | 0.153 (0.150–0.153) | 0.110 (0.107–0.112) | 0.109 (0.102–0.120) |
| boxed DATE / list / 262,144 | 0.647 (0.645–0.668) | 1.175 (1.167–1.189) | 0.663 (0.648–0.683) | 0.665 (0.648–0.704) |
| date-sql:array / 262,144 | 0.100 (0.094–0.105) | 0.198 (0.194–0.207) | 0.097 (0.094–0.107) | 0.097 (0.094–0.108) |
| time-integer:array / 262,144 | 0.094 (0.091–0.102) | 0.193 (0.190–0.202) | 0.094 (0.090–0.101) | 0.097 (0.089–0.102) |
| time-long:array / 262,144 | 0.095 (0.092–0.100) | 0.196 (0.192–0.204) | 0.105 (0.096–0.106) | 0.096 (0.094–0.105) |
| time-sql:array / 262,144 | 0.099 (0.099–0.108) | 0.199 (0.195–0.200) | 0.101 (0.096–0.107) | 0.100 (0.096–0.106) |
| timestamp-sql:array / 262,144 | 0.109 (0.108–0.116) | 0.206 (0.206–0.210) | 0.109 (0.106–0.110) | 0.108 (0.107–0.114) |
| ltz-integer:array / 262,144 | 0.097 (0.096–0.106) | 0.199 (0.199–0.209) | 0.097 (0.092–0.107) | 0.095 (0.094–0.098) |
| ltz-long:array / 262,144 | 0.096 (0.094–0.098) | 0.203 (0.202–0.203) | 0.103 (0.098–0.108) | 0.104 (0.096–0.107) |
| ltz-sql:array / 262,144 | 0.100 (0.098–0.107) | 0.204 (0.202–0.212) | 0.098 (0.096–0.099) | 0.099 (0.098–0.107) |
| date-sql:list / 262,144 | 0.632 (0.626–0.634) | 1.990 (1.971–1.996) | 0.636 (0.634–0.639) | 0.631 (0.626–0.634) |
| timestamp-sql:list / 262,144 | 0.639 (0.635–0.652) | 2.010 (1.999–2.018) | 0.696 (0.685–0.701) | 0.702 (0.684–0.712) |
| timestamp-sql:list / 2,097,152 | 16.908 (12.123–57.638) | 24.214 (23.209–73.133) | 12.145 (12.137–12.157) | 12.152 (12.145–12.166) |

## Reproduction

Prepare the runtime classes with `mvn -pl streamfusion-runtime -Pbench test-compile`, then run:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench -Dnative.build.skip=true \
  -Dtest=BoxedDateTableFunctionBenchmark,AlternateTemporalTableFunctionBenchmark \
  -Dsf.testForks=1 '-Dsf.benchmarkJvmArgs=-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false' \
  -Dboundary.rows=262144 -Dboundary.warmups=2 -Dboundary.runs=5 \
  surefire:test@default-test
```

Add `,flink-1.18` to the Maven profile for Flink 1.18.1 and rebuild its classes before timing.
For the larger case select `AlternateTemporalTableFunctionBenchmark`, set
`-Dtemporal.bridge.benchcases=timestamp-sql:list` and `-Dboundary.rows=2097152`.
For previous production, check out `f5274a10`, copy only the four new fixtures named in the
artifact, prepare that revision's classes with the same profile, and set
`-Dboundary.expectNative=false`. Reuse the immutable native library through
`native.resource.directory`/`java.library.path` when working in a separate checkout; verify
its hash and that the production native sources agree. Do not compile or run other tests
concurrently with measurements. The retained revision hashes define the measured workload;
additional bridge cases added later require their own named measurements.
