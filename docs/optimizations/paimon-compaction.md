# Columnar Paimon compaction

Java Paimon chooses compaction files, sections, output levels and delete retention. One shared
payload implementation handles Parquet/ORC primary-key deduplication and append compaction across
the released 2.1/Flink 2.2, 2.0/Flink 1.18 and 1.0/Flink 1.18 adapters. Released paimon-rust reads
and merges the selected files; the existing native codecs encode Arrow bundles. Java retains
metadata, rolling, abort, scheduling and commit/recovery. No comparator, reducer or compaction
planner is added. [Coverage and fallback](../connectors/paimon.md#compaction) remain bounded.

## Resource-aware append rewrites

Append admission reserves the estimated reader, encoder and staging working set against the shared
TaskManager authority before output opens, leaving headroom for independently owned Arrow imports.
The released paimon-rust resource context suballocates the reader allowance. One row group runs at
a time, and the encoder's retained-buffer estimate is checked after each bundle. This admits ordinary
native-written Parquet groups when task resources allow them, without treating the unrelated sort
buffer as the compact reader's budget. See [the coverage page](../connectors/paimon.md#compaction)
for the formula, fallback evidence, ownership and limitations.

Compact reads reuse one staging buffer, grown lazily up to the smaller of the projected group estimate
and 4 MiB (with a 64 KiB floor), across streams and sequential files. The OpenDAL positioned reader
uses that same bound. Previously, each native range was fragmented into 2 MiB positioned reads and
64 KiB Java stream copies. Matching both limits reduces JNI and underlying stream-read overhead;
larger Java buffers alone leave OpenDAL's fragmentation in place. Ordinary sources keep their
existing transfer sizes. This does not claim parallel remote I/O from the synchronous Java FileIO.

The [complete observations](../benchmarks/results/paimon-append-memory-638.json) retain all 168
trials, including warmups, on Apple M4 / 16 GiB, JDK 17.0.17, Flink 2.2.1, Paimon 2.1.0 and
paimon-rust 0.4.0 with release/mimalloc libraries and one codegen unit. Each engine has two active JVM
processors, a 2 GiB heap and 1 GiB available task budget. Two actual native-written inputs contain
128 plus 16,384/49,152 rows of approximately 1 KiB unique strings, NULLs and three partition fields.
The original append rewriter from `e1d71357c` supplies the previous policy; only its class name changes.
All engines reuse the same files, rotate order, warm up twice and run five measured trials.

Medians in milliseconds; each cell's full range and individual observations are in the JSON:

| Rows | Row-group target | Added read latency | Stock Java | Previous | Current |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 16,512 | 128 MiB | 0 | 177.373 | 148.938 | 146.576 |
| 16,512 | 8 MiB | 0 | 177.118 | 147.743 | 154.351 |
| 49,280 | 128 MiB | 0 | 499.979 | 495.901 | 414.043 |
| 49,280 | 8 MiB | 0 | 485.640 | 413.524 | 419.294 |
| 16,512 | 128 MiB | 1 ms/read | 197.901 | 498.275 | 175.084 |
| 16,512 | 8 MiB | 1 ms/read | 202.260 | 503.086 | 175.867 |
| 49,280 | 128 MiB | 1 ms/read | 527.923 | 535.157 | 450.198 |
| 49,280 | 8 MiB | 1 ms/read | 522.189 | 1,380.905 | 455.899 |

The reported default-group reproduction now runs natively: elapsed medians improve 17.2% against
stock and 16.5% against the previous Java fallback without added latency; with synthetic latency,
they improve 14.7% and 15.9%. Every current case beats stock in this series. Already-native 8 MiB
controls cost 1.4–4.5% more than previous without added latency (ranges overlap), while the latency
controls improve substantially. This is a bounded resource policy with measured costs, not a
uniform speedup over every previous native path.

The latency diagnostic delays each underlying Java stream read by one millisecond only inside
timing. It is not a remote-store measurement. Initial 64 KiB transfers lost heavily under that
diagnostic; a larger Java buffer alone still trailed stock. The final comparison includes both
transfer limits. Spotlight consumed approximately one CPU core during the final series, so absolute
times must not be compared with earlier pilots or presented as idle-machine throughput. Within each
series data, limits, host conditions and verification are shared across engines.

The timer includes preflight, runtime setup, JNI, FileIO, decode, memory admission, encode and output
close; creating the table-writer object, fixtures, full value/statistics validation and cleanup are
outside it. Every content value, NULL, partition field, sequence and statistic must match; footer
witnesses and counters require the expected native/Java routes. These are local file-rewrite
diagnostics, not complete-job, startup, remote-storage, RSS or CPU-efficiency claims.
The [coverage page](../connectors/paimon.md#compaction) includes reproduction commands.

The [expanded release diagnostic](../benchmarks/paimon-compaction-expanded-20261105/README.md)
retains 120 worker trials on Paimon 1.0: Parquet medians improve 1.27–2.04× versus stock and
1.26–2.02× versus previous; ORC improves 1.27–1.67× versus stock and 1.30–1.69× versus previous.
The ORC run uses the same larger sort-spill budget for all engines. The wide nested PK shape
retains Java at the smaller budget. These figures do not establish the new combinations' complete-job
latency or the other adapters' performance.

## Original Flink 2.2 / Paimon 2.1 Parquet comparison

Measured on 2026-11-04, Apple M4 (10 CPUs), 16 GiB RAM, JDK 17.0.17, Flink 2.2.1, Java Paimon
2.1.0 and crates.io paimon 0.4.0. All native libraries use release/mimalloc builds. The JVM has
an 8 GiB maximum heap; each table has one bucket, a 256 MiB write buffer, a 256 MiB sort-spill
buffer, 4 MiB Parquet row groups, default compression and 1,024-row reads. Native output is handed
to the shared rolling writer in chunks of at most 4,096 rows. Limits and input files
are identical across engines. These runs do not measure per-engine peak RSS and make no memory
efficiency claim.

`stock` uses canonical Paimon format factories. `previous` retains the prior StreamFusion format
shim and Java compaction, on the same released 2.1 dependency. `native` installs the new decorator.
The test agent selects these baselines; production has no performance opt-in.

Each shape starts from the same stock-written files and runs one warmup plus three measured
trials, rotating engine order. Unique keys are interleaved across four runs. Versioned shapes
write the same keys four times, with 20% winning deletes. Wide rows add a nullable string and
an integer array containing a null. Rolling uses a 512 KiB target instead of 128 MiB. Multilevel
first compacts two runs with stock Paimon, then adds two L0 runs; its physical input is 75% of
the stated input-change count. Other shapes retain all input changes as physical records.

The worker timer includes real table-writer creation, Java file selection, authenticated IO,
JNI, decode/merge/encode, metadata, commit preparation and writer close. Fixture creation,
validation and deletion of uncommitted outputs are outside it. Stock readers reopen every output
and compare SHA-256 of complete physical key-value rows, including stored sequences and kinds.
Footer witnesses require parquet-mr for both Java baselines and parquet-rs for native output.
Every native trial requires positive native groups and zero fallback groups. The outputs may
roll at different boundaries: at 2M changes the rolling shape produced 22 Java files and 24
native files with the same configured size policy and identical physical rows.

Medians in seconds, with all three observations' ranges:

| Changes | Shape | Stock median (range) | Previous median (range) | Native median (range) | Stock/native |
| --- | --- | --- | --- | --- | --- |
| 400k | unique | 0.661 (0.658–0.669) | 0.664 (0.660–0.708) | 0.476 (0.460–0.487) | 1.39× |
| 400k | versions | 0.282 (0.276–0.294) | 0.275 (0.270–0.279) | 0.168 (0.168–0.176) | 1.68× |
| 400k | wide | 0.517 (0.512–0.519) | 0.520 (0.520–0.526) | 0.312 (0.296–0.313) | 1.66× |
| 400k | rolling | 0.354 (0.352–0.356) | 0.359 (0.358–0.361) | 0.182 (0.181–0.190) | 1.94× |
| 400k | multilevel | 0.236 (0.235–0.239) | 0.236 (0.235–0.236) | 0.144 (0.143–0.144) | 1.64× |
| 2M | unique | 0.662 (0.660–0.666) | 0.663 (0.657–0.664) | 0.429 (0.424–0.436) | 1.54× |
| 2M | versions | 0.259 (0.258–0.262) | 0.265 (0.263–0.266) | 0.161 (0.161–0.165) | 1.61× |
| 2M | wide | 0.513 (0.513–0.519) | 0.518 (0.515–0.540) | 0.285 (0.285–0.287) | 1.80× |
| 2M | rolling | 0.352 (0.352–0.355) | 0.351 (0.348–0.363) | 0.183 (0.180–0.187) | 1.93× |
| 2M | multilevel | 0.224 (0.221–0.226) | 0.223 (0.223–0.224) | 0.145 (0.142–0.151) | 1.55× |

Native worker medians are **1.39–1.94×** faster than stock and **1.39–1.97×** faster than previous
StreamFusion in these cases. These are file-worker diagnostics, not Nexmark throughput claims.
Cold work can lose: the first 400k unique-key trial took 1.570 s native versus 0.931 s stock.
All warmup observations remain in the [worker CSV](../benchmarks/paimon-compaction-20261104/worker.csv).
The [earlier pilot](../benchmarks/paimon-compaction-20261104/pilot-worker.csv) used a weaker checksum
and four shapes; it is retained separately rather than mixed into the final SHA-256 comparison.

## Running maintenance job

A separate diagnostic starts an ordinary continuous full-data compaction job, waits for RUNNING,
lets its empty source and checkpoints settle for 500 ms, then publishes the identical prebuilt
files in one stock append commit. The timer includes that publication, source discovery, payload
work, checkpoint and the resulting compacted snapshot. Discovery is 50 ms, checkpoints 100 ms,
and parallelism one for every engine. Full committed rows must equal the stock input snapshot;
native output also requires Rust footer witnesses. Startup plus the common settling wait is
retained separately in the [job CSV](../benchmarks/paimon-compaction-20261104/jobs.csv).

| Changes | Shape | Stock median (range) | Previous median (range) | Native median (range) | Stock/native |
| --- | --- | --- | --- | --- | --- |
| 400k | versions | 0.491 (0.479–0.492) | 0.476 (0.427–0.489) | 0.390 (0.380–0.392) | 1.26× |
| 400k | wide | 0.732 (0.706–0.778) | 0.714 (0.699–0.756) | 0.513 (0.477–0.514) | 1.43× |
| 400k | multilevel | 0.498 (0.477–0.504) | 0.422 (0.421–0.492) | 0.351 (0.311–0.406) | 1.42× |
| 2M | versions | 0.430 (0.406–0.484) | 0.477 (0.400–0.492) | 0.379 (0.331–0.476) | 1.13× |
| 2M | wide | 0.761 (0.720–0.781) | 0.744 (0.732–0.791) | 0.545 (0.524–0.592) | 1.40× |
| 2M | multilevel | 0.471 (0.397–0.518) | 0.447 (0.437–0.475) | 0.335 (0.294–0.355) | 1.41× |

Publication-to-commit medians improved **1.13–1.43×** versus stock and **1.20–1.39×** versus previous
StreamFusion. Polling and checkpoint alignment contribute substantial variance, including slower
individual native observations. The small 40k smoke diagnostic was dominated by that cadence
and did not consistently improve latency; no speedup is claimed for it. MiniCluster startup,
input file creation and post-commit full-row validation are outside this running-job timer.

## Reproduce

Use JDK 17 and build the test agent first. This command builds matching release libraries unless
`-Dnative.build.skip=true` is supplied after an explicit matching native build.

```bash
mvn -B -ntp -f dev/flink-suite/agent/pom.xml package
SF_PAIMON_COMPACTION_BENCHMARK=true \
SF_PAIMON_COMPACTION_JOB_BENCHMARK=true \
SF_PAIMON_COMPACTION_ROWS=400000 SF_PAIMON_COMPACTION_REPEATS=3 \
mvn -B -ntp -Ppaimon,bench -pl streamfusion-paimon -am test \
  -Dtest=PaimonCompactionBenchmark,PaimonCompactionJobBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false \
  "-Dsf.extraJvmArgs=-javaagent:$PWD/dev/flink-suite/agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar -Dstreamfusion.paimon.compaction.mode=native -Dstreamfusion.flink-suite.sql-inventory=$PWD/streamfusion-paimon/target/compaction-benchmark-inventory"
```

Repeat with `SF_PAIMON_COMPACTION_ROWS=2000000`. The two diagnostics have separate environment
switches so worker throughput and running-job latency can also be measured independently.
The unchanged upstream correctness corpus has its own [runner and work contracts](../upstream-flink-suite.md#paimon-compaction-fixtures).
