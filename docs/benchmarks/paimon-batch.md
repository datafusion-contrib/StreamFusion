# Bounded Paimon connector boundaries

`PaimonBatchBoundaryBenchmark` compares complete bounded Paimon copies with stock Flink,
the previous StreamFusion connector boundaries, and native reading/routing/writing. The
previous-boundary control uses the current engine with both Paimon substitutions disabled;
it is an ablation rather than a rebuilt historical release.

All engines read the same stock-written snapshot. Input has seven physical partitions,
unique integer IDs, an integer partition key and nullable strings of approximately 64 bytes
with 4,096 distinct non-null values. One in 17 strings is null. The unchanged query copies
rows with `id >= inputRows / 4`, retaining three quarters of the input. Output modes cover
bucket-unaware append tables and primary-key tables with two fixed buckets, dynamic buckets,
or bounded postpone fixed-bucket writes. Parquet and ORC both use Snappy and write-only tables.

The timer starts before SQL submission and ends after the successful final commit. It retains
planning, startup, file reading, JNI, Arrow/row boundaries, shuffle, sorting/merging and encoding.
Fixture preparation, output schema creation and verification are outside timing. Verification
reads every committed row through stock Paimon, checks its ID, partition, payload and nulls,
and rejects missing or duplicate IDs. Every native run must report native decoding, no source
fallback, positive native sink routing, zero entry transposes and native file encoders.
Both controls must have stock encoders and no native source work.

Release builds use mimalloc, parallelism two, 8,192-row Arrow batches, IPC exchanges and
a 512 MiB native memory authority. Each case has two warmups and five measured observations
per engine, rotating execution order. Report elapsed and net job times separately; these
bounded jobs include startup and do not measure sustained streaming throughput.
The reporter retains counters, while dropping gauges that can keep completed operators and
writer buffers alive. An earlier instrumentation pilot retained all gauges and filled its
8 GiB heap during the million-row cohort; those pilot observations are kept separately and
are not mixed with the corrected measurements.

On Flink 2.2.1 / Paimon 2.1.0 on Apple Silicon with JDK 17.0.17, all 336 jobs (including warmups) passed exact committed-row
and encoder checks. Native medians beat both controls in all 16 cases. The append observations were refreshed after preserving row-level round-robin routing;
the earlier whole-batch-routing observations remain separately labeled in the JSON. The corrected million-row
run completed without the instrumentation pilot's heap exhaustion. Median milliseconds
(minimum–maximum), including submission and commit:

| Input rows | Format / output | Stock | Previous boundaries | Native | Stock / native |
| ---: | --- | ---: | ---: | ---: | ---: |
| 250,000 | parquet / append | 208.1 (133.4–242.3) | 220.5 (141.1–260.3) | 93.1 (89.3–107.2) | 2.23× |
| 250,000 | parquet / fixed | 478.3 (397.7–589.8) | 439.7 (368.5–497.8) | 128.6 (114.1–188.0) | 3.72× |
| 250,000 | parquet / dynamic | 617.5 (358.1–846.3) | 555.0 (399.1–695.3) | 301.6 (157.6–346.5) | 2.05× |
| 250,000 | parquet / postpone | 463.1 (384.5–510.3) | 442.9 (360.2–523.4) | 101.6 (94.9–114.1) | 4.56× |
| 250,000 | orc / append | 209.0 (198.5–232.6) | 219.3 (191.2–280.5) | 93.0 (87.6–163.0) | 2.25× |
| 250,000 | orc / fixed | 378.1 (305.0–405.6) | 348.3 (312.0–393.1) | 174.4 (157.9–193.8) | 2.17× |
| 250,000 | orc / dynamic | 406.1 (379.8–486.3) | 401.8 (347.0–438.8) | 215.7 (209.2–248.2) | 1.88× |
| 250,000 | orc / postpone | 485.0 (415.1–530.6) | 516.4 (413.4–549.7) | 179.5 (170.1–229.2) | 2.70× |
| 1,000,000 | parquet / append | 370.9 (336.8–384.2) | 355.2 (334.9–361.9) | 193.4 (192.0–206.5) | 1.92× |
| 1,000,000 | parquet / fixed | 953.5 (932.7–1051.7) | 992.9 (931.8–1035.3) | 351.7 (306.4–405.0) | 2.71× |
| 1,000,000 | parquet / dynamic | 1434.5 (1276.2–1534.1) | 1511.6 (1403.7–1591.1) | 717.9 (656.8–733.4) | 2.00× |
| 1,000,000 | parquet / postpone | 1733.2 (1674.6–1919.7) | 1835.8 (1653.4–2122.3) | 408.8 (381.3–481.3) | 4.24× |
| 1,000,000 | orc / append | 355.8 (349.0–368.6) | 368.6 (353.5–398.3) | 265.6 (260.1–273.3) | 1.34× |
| 1,000,000 | orc / fixed | 1579.3 (1400.0–1894.6) | 1556.9 (1365.6–1723.5) | 643.6 (629.1–716.0) | 2.45× |
| 1,000,000 | orc / dynamic | 2015.3 (1702.3–2130.9) | 1852.7 (1558.3–1988.1) | 1121.5 (923.8–1431.2) | 1.80× |
| 1,000,000 | orc / postpone | 2125.6 (1750.0–3029.5) | 2041.6 (1926.9–3269.0) | 655.4 (554.9–805.9) | 3.24× |

[Individual observations, ranges and library checksums](results/paimon-batch-boundaries-20261113.json)
retain every warmup and measured trial. The [retained-metrics pilots](results/paimon-batch-retained-metrics-pilot-20261113.json)
include the aborted million-row cohort and are kept separate from these results.

Flink 1.18.1 / Paimon 2.0.0 on the same Apple Silicon host with JDK 11.0.28 also passed
all 168 jobs at 250,000 input rows. Every native median beat stock (1.42–3.14×) and the
previous boundary control (1.50–2.79×). Median milliseconds (minimum–maximum):

| Format / output | Stock | Previous boundaries | Native | Stock / native |
| --- | ---: | ---: | ---: | ---: |
| parquet / append | 198.5 (191.6–241.7) | 209.9 (200.5–231.3) | 139.6 (136.0–152.4) | 1.42× |
| parquet / fixed | 1087.1 (987.0–1382.7) | 1056.1 (950.6–1351.3) | 427.4 (332.7–495.5) | 2.54× |
| parquet / dynamic | 1066.9 (983.4–1211.0) | 1135.3 (978.1–1291.3) | 542.1 (509.3–588.4) | 1.97× |
| parquet / postpone | 1052.0 (919.3–1306.1) | 932.7 (845.4–1029.9) | 334.8 (300.8–401.1) | 3.14× |
| orc / append | 219.8 (201.3–234.2) | 229.4 (204.8–278.5) | 151.4 (143.1–166.2) | 1.45× |
| orc / fixed | 265.4 (257.9–275.0) | 265.2 (257.4–296.9) | 156.7 (152.6–159.4) | 1.69× |
| orc / dynamic | 312.0 (290.8–359.1) | 321.2 (299.0–329.1) | 203.9 (199.1–209.1) | 1.53× |
| orc / postpone | 289.4 (270.6–299.6) | 300.9 (259.7–328.7) | 160.3 (147.6–184.6) | 1.80× |

Together the three corrected cohorts contain 504 validated jobs. The observations identify
both Flink/Paimon versions; do not combine timings across versions into one speedup.

Run with JDK 17 and released native libraries:

```sh
SF_BENCHMARK=true mvn -Ppaimon,bench -pl streamfusion-paimon -am test \
  -Dtest=PaimonBatchBoundaryBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dsf.extraJvmArgs=-Dsf.paimon.batch.rows=250000 -Dsf.paimon.batch.report=paimon-batch.json'
```

Repeat with `sf.paimon.batch.rows=1000000`. `sf.paimon.batch.formats` selects `parquet,orc`;
`sf.paimon.batch.buckets` selects `append,2,-1,-2`. Test-owned fixtures are deleted after each run.

## TPC-H inputs and committed outputs

The `PaimonTpchBenchmark` harness runs the unchanged 22-query TPC-H workload with stock-created
Paimon inputs and committed Paimon result tables. Its inputs and outputs are **unpartitioned**
bucket-unaware append tables (`bucket=-1`), using Snappy Parquet. A `bucket-0` storage directory
does not imply a configured fixed bucket count. Execution parallelism is two, and admitted
joins and aggregates retain their exchanges independently of table partitioning.

Both engines use identical input snapshots. Each result is read back through stock Paimon and
compared as a complete typed row multiset; physical table ordering is not observable. Preparation,
schema creation and readback are excluded from timing. The final commit is included. Reports
record partition keys, observed partitions, bucket modes, file counts and encoder identity.

```sh
SF_TPCH=true SF_BENCHMARK=true mvn -Ptpch,paimon,bench -pl streamfusion-paimon -am test \
  -Dtest=PaimonTpchBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dsf.extraJvmArgs=-Dsf.tpch.directory=/path/to/task-owned-directory -Dsf.tpch.mode=compare -Dsf.tpch.scale=1'
```

For a correctness control, use `sf.tpch.mode=verify` and `sf.tpch.scale=0.01`. The legacy
adapter selects `-Pflink-1.18,tpch,paimon1,bench -pl streamfusion-paimon1`. Such small-scale
verification timings are not a performance cohort.

Both [Flink 2.2/Paimon 2.1](results/paimon-batch-tpch-validation22-20261113.json) and
[Flink 1.18/Paimon 1.0](results/paimon-batch-tpch-validationlegacy-20261113.json) passed all
22 queries at SF0.01. Every native job had native source/compute work, zero entry transposes,
no source decode fallback and native encoders for nonempty outputs. Stock readback verified
the complete declared result types and committed row multisets.

## Java batch operations and native Parquet encoding

`PaimonBatchOperationsBenchmark` compares complete partitioned overwrite, SQL UPDATE/DELETE and
clustering jobs. The three controls are stock Flink, the previous StreamFusion behavior (native
source/compute with `paimonBatchEncoding` disabled), and native file encoding. Each trial includes
Java sorting/mutation/merge logic, Arrow conversion, JNI encoding, Java IO and the successful final
commit. Fixtures and exact stock SDK readback are outside the timer. Alternating engines share the
same data and resources; warmup observations are retained separately. Parquet footer `created_by`
and source counters establish actual execution. Results must be measured with the release `bench`
profile. Configure `sf.paimon.operations.rows`, `.warmups`, `.trials`, `.cases`, and `.report` to
select the input size, repetitions, operation set and structured report destination.

### Initial codec cohorts

The following observations precede direct string copying and full 4,096-row buffering.
They establish the initial implementation's behavior, rather than its final performance.
Later repeats and unfavorable observations remain in the same structured report.

On Apple Silicon, JDK 17.0.17, Flink 2.2.1 and Paimon 2.1.0, both initial cohorts completed all 105
jobs (two warmups and five measured trials per engine and operation). The 210 jobs passed exact
stock SDK row validation and native-source/encoder checks. Each engine used two writers, seven
table partitions, Snappy, an 8 GiB maximum heap, a 512 MiB native budget and Arrow IPC shuffles.
The schema is two integers and a nullable 61–64-byte string with 4,096 distinct values. Java
owns all file IO, sorting, mutation/merge semantics and final commits in every engine.

Median elapsed milliseconds (minimum–maximum), including submission and commit:

| Input rows | Operation | Stock | Previous | Native encoding | Stock / native |
| ---: | --- | ---: | ---: | ---: | ---: |
| 250,000 | overwrite append | 371.5 (292.9–384.7) | 246.0 (233.4–296.3) | 242.6 (189.5–257.5) | 1.53× |
| 250,000 | overwrite primary key | 410.8 (340.3–440.5) | 346.5 (316.2–395.2) | 273.5 (259.5–326.7) | 1.50× |
| 250,000 | UPDATE | 322.3 (271.9–372.7) | 277.7 (255.9–310.1) | 267.0 (220.1–298.5) | 1.21× |
| 250,000 | DELETE | 287.2 (272.8–356.2) | 283.8 (280.3–324.0) | 281.3 (219.0–285.8) | 1.02× |
| 250,000 | clustering insert | 311.3 (275.9–328.3) | 311.5 (242.4–359.5) | 281.9 (190.1–312.6) | 1.10× |
| 1,000,000 | overwrite append | 332.2 (322.8–337.0) | 324.1 (314.1–329.0) | 246.2 (237.7–260.0) | 1.35× |
| 1,000,000 | overwrite primary key | 520.3 (511.9–552.1) | 496.6 (496.0–508.3) | 439.2 (432.9–444.0) | 1.18× |
| 1,000,000 | UPDATE | 448.9 (425.9–467.9) | 458.5 (443.3–467.7) | 436.1 (409.2–436.7) | 1.03× |
| 1,000,000 | DELETE | 523.3 (513.0–528.6) | 499.7 (484.7–511.2) | 455.6 (433.9–475.1) | 1.15× |
| 1,000,000 | clustering insert | 914.7 (667.8–1174.1) | 729.8 (649.9–1051.3) | 663.3 (603.9–850.2) | 1.38× |

Native medians beat both controls in these ten comparisons. Gains over previous behavior range
from 1.009× to 1.317×; 250,000-row DELETE is marginal and the trial ranges overlap. These short
bounded jobs include startup and commit costs, so interpret the results at that scope. The
[structured observations](results/paimon-batch-operations-20261114.json) retain warmups, every
trial, net job runtime, encoder evidence and native library checksums.

Flink 1.18.1/Paimon 2.0.0 with JDK 11.0.28 passed all 150 jobs in a follow-up cohort with
three warmups and seven measured trials per engine/operation. Native medians beat stock by
1.10–1.17× and previous behavior by 1.05–1.22×. Initial DELETE measured 300.0 ms versus stock
295.5 ms (0.985×), while its net job runtime was consistently lower. A focused JFR diagnostic
also showed faster native worker runtimes. Planning now creates one runtime table copy rather
than copying it again solely to add the encoder marker. The final unprofiled DELETE medians
were stock 281.5 ms, previous 272.9 ms, native 255.9 ms. This repeat does not establish how much
of the change came from planning versus host/JVM variability. The JSON retains the initial
cohort, the profiling diagnostic and the final cohort separately.

The released Paimon 1.0.0 adapter on Flink 1.18.1/JDK 11.0.28 also completed all 150 jobs
with three warmups and seven measured trials. All five native medians beat stock (1.02–1.30×)
and previous behavior (1.06–1.21×), with exact stock-reader results and native Parquet footers.
The UPDATE result is marginal and its trial ranges overlap. Its observations remain a separate
versioned cohort in the same JSON.

### String copying and buffered size checks

The row adapter now copies ordinary heap-backed Paimon strings directly into Arrow and
keeps its 4,096-row buffer across SDK size probes when it remains safely below the target
file size. It flushes near the roll boundary and at close, with an 8 MiB allocation
threshold for wide values. Unicode, offset/multisegment strings, NULLs, reused input rows,
wide buffers, close and encoding failure have dedicated ownership controls.

On Flink 2.2.1/Paimon 2.1.0, the 250,000-row cohort with three warmups and seven measured
trials per engine passed all 150 jobs. The native medians beat stock by 1.08–1.22× and
previous behavior by 1.07–1.19×. Earlier repeats had losses in append, DELETE, UPDATE and
clustering; those observations are retained and are not mixed into this cohort.

A subsequent Flink 1.18.1/Paimon 2.0.0 cohort passed the same 150 correctness and encoder
checks. Four native medians beat both controls; clustering was slower. Median milliseconds
(minimum–maximum), including submission and commit:

| Operation | Stock | Previous | Native encoding | Stock / native |
| --- | ---: | ---: | ---: | ---: |
| overwrite append | 287.9 (245.9–411.8) | 280.9 (224.1–339.1) | 251.9 (200.7–327.2) | 1.14× |
| overwrite primary key | 371.9 (338.8–408.3) | 382.1 (315.0–422.3) | 322.3 (295.3–359.2) | 1.15× |
| UPDATE | 311.3 (299.1–359.7) | 316.8 (291.1–379.8) | 297.4 (275.8–371.2) | 1.05× |
| DELETE | 407.6 (374.5–434.9) | 399.9 (347.4–455.4) | 347.2 (295.1–417.4) | 1.17× |
| clustering insert | 394.1 (355.9–884.7) | 391.9 (322.1–479.7) | 471.9 (286.4–745.9) | 0.84× |

These are short jobs on a shared Apple M4 host with 16 GiB RAM, and their ranges overlap.
The clustering loss requires further profiling; these observations do not establish a
general gain for every adapter and operation.
