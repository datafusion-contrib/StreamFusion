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

## Additional boxed interval bridges

The [interval artifact](results/jvm-table-function-interval-bridges-2026-11-01.json) retains
all trials for Integer months and Long milliseconds on both releases. It measures immutable
`fa31713d` (integrated as `7c866310`) against unchanged `f5274a10`, with the same released DSO
and complete-job boundaries. Each of the 18 selected correctness controls passes per line.
The following 262,144-row jobs use ARRAY containers; medians and full measured ranges are seconds.

| Flink / case | Stock in candidate JVM | Candidate native | Stock in previous JVM | Previous enabled fallback |
| --- | --- | --- | --- | --- |
| 2.2 / yearmonth-integer:array | 0.100 (0.091–0.102) | 0.202 (0.194–0.208) | 0.097 (0.085–0.100) | 0.095 (0.084–0.098) |
| 2.2 / daytime-long:array | 0.100 (0.091–0.109) | 0.189 (0.183–0.193) | 0.097 (0.090–0.104) | 0.098 (0.093–0.100) |
| 1.18 / yearmonth-integer:array | 0.098 (0.095–0.099) | 0.195 (0.191–0.196) | 0.096 (0.096–0.097) | 0.096 (0.090–0.098) |
| 1.18 / daytime-long:array | 0.091 (0.089–0.101) | 0.178 (0.177–0.187) | 0.098 (0.090–0.099) | 0.098 (0.088–0.104) |

Reproduce with the same benchmark command, selecting `AlternateTemporalTableFunctionBenchmark`
and `-Dtemporal.bridge.benchcases=yearmonth-integer:array;daytime-long:array` (quote the
property argument in a shell). Add `,flink-1.18` for that line. These slower native costs remain
accepted UDF coverage, not an acceleration claim.

## Primitive temporal bridges and custom scalar inference

The [follow-up artifact](results/jvm-function-contract-followups-2026-11-01.json) records
all warmup/measured trials for six primitive argument/Row-result bridges and two custom
nested-scalar inference forms. It compares immutable `5fa5ceae` with pre-goal canonical
`f5274a10` using identical fixture sources and the same release/mimalloc DSO. Previous
production sources remain unchanged. Timed jobs use the same resources, alternating pairs,
row source/blackhole and both transposes as above. The combined correctness selection passes
252 checks on 2.2.1 and 251 plus the existing ARRAY_SORT N/A on 1.18.1, without failures/errors.

These 262,144-row observations retain startup/JIT effects and sampled background activity.
Native UDF costs are slower here; these are admitted under the accepted UDF performance
exception. Independent stock controls remain separate rather than presenting their drift
as an isolated effect. All measurements and unfavorable trials are retained.

| Flink / case | Stock in candidate JVM | Candidate native | Stock in previous JVM | Previous enabled fallback |
| --- | --- | --- | --- | --- |
| 2.2 / inferred explicit | 0.373 (0.371–0.375) | 0.566 (0.560–0.569) | 0.371 (0.368–0.375) | 0.374 (0.367–0.374) |
| 2.2 / inferred delegating | 0.376 (0.366–0.377) | 0.566 (0.563–0.585) | 0.376 (0.375–0.398) | 0.375 (0.374–0.375) |
| 2.2 / primitive time-int | 0.090 (0.084–0.093) | 0.171 (0.165–0.173) | 0.087 (0.081–0.092) | 0.088 (0.085–0.098) |
| 2.2 / primitive time-primitive-long | 0.088 (0.081–0.091) | 0.172 (0.168–0.174) | 0.090 (0.081–0.092) | 0.089 (0.080–0.091) |
| 2.2 / primitive ltz-int | 0.082 (0.079–0.096) | 0.172 (0.162–0.178) | 0.087 (0.078–0.091) | 0.082 (0.080–0.091) |
| 2.2 / primitive ltz-primitive-long | 0.084 (0.083–0.089) | 0.170 (0.169–0.178) | 0.084 (0.079–0.091) | 0.086 (0.081–0.098) |
| 2.2 / primitive yearmonth-int | 0.091 (0.081–0.094) | 0.164 (0.163–0.166) | 0.083 (0.082–0.090) | 0.093 (0.086–0.111) |
| 2.2 / primitive daytime-primitive-long | 0.085 (0.082–0.094) | 0.166 (0.164–0.172) | 0.084 (0.082–0.091) | 0.082 (0.080–0.092) |
| 1.18 / inferred explicit | 0.382 (0.375–0.384) | 0.568 (0.565–0.575) | 0.370 (0.365–0.372) | 0.368 (0.366–0.372) |
| 1.18 / inferred delegating | 0.383 (0.379–0.392) | 0.567 (0.564–0.568) | 0.373 (0.368–0.375) | 0.372 (0.370–0.374) |
| 1.18 / primitive time-int | 0.088 (0.086–0.090) | 0.167 (0.167–0.170) | 0.085 (0.083–0.089) | 0.088 (0.084–0.093) |
| 1.18 / primitive time-primitive-long | 0.090 (0.081–0.091) | 0.169 (0.166–0.169) | 0.089 (0.080–0.090) | 0.088 (0.080–0.090) |
| 1.18 / primitive ltz-int | 0.090 (0.088–0.091) | 0.172 (0.162–0.174) | 0.089 (0.086–0.089) | 0.081 (0.079–0.090) |
| 1.18 / primitive ltz-primitive-long | 0.088 (0.082–0.091) | 0.170 (0.162–0.174) | 0.089 (0.081–0.090) | 0.090 (0.089–0.091) |
| 1.18 / primitive yearmonth-int | 0.091 (0.080–0.101) | 0.161 (0.159–0.168) | 0.091 (0.081–0.092) | 0.090 (0.080–0.091) |
| 1.18 / primitive daytime-primitive-long | 0.083 (0.080–0.093) | 0.168 (0.162–0.172) | 0.090 (0.088–0.097) | 0.092 (0.089–0.094) |

Reproduce with the same `-Pbench` command and `-Dtest=PrimitiveTemporalTableFunctionBenchmark,InferredNestedScalarBenchmark`.
Add `,flink-1.18` for that line. For the baseline copy only the six test fixtures listed
in the artifact and use `-Dboundary.expectNative=false`; leave production sources unchanged.

A subsequent NOT NULL metadata check adds three controls without production changes: matching
root-NULL sink failure, released non-null-child behavior and successful filtered non-null roots.
The final selection passes 255 checks on 2.2.1 and 254 plus the existing ARRAY_SORT N/A on
1.18.1. The artifact keeps the original measurement revision and records extended validation
separately; the timed fixture bodies are unchanged.

The follow-up measurement used a local pre-rebase commit. Production sources are identical
to the rebased revision recorded in the artifact. For byte-for-byte fixture reproduction,
check out that rebased revision and apply the [measured-fixture patch](sources/jvm-contract-followup-measured-fixtures.patch)
before copying fixtures into the baseline and compiling. It removes only the later NOT NULL
controls; verify the recorded fixture hashes. This preserves public reproduction after rebasing
onto the parent coverage merge without changing measured production code.
