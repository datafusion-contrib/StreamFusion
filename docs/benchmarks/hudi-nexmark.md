# Four-partition Hudi Nexmark

`NexmarkHudiBenchmark` uses the existing Nexmark matrix queries and Kafka event
corpus with Hudi tables at both ends. This is a separate storage workload from
the README Kafka benchmark. It requires the released Hudi 1.2.1 Flink 1.18 bundle
and release StreamFusion engine/Parquet libraries.

For the Java batching/allocation investigation, the manual **CI startup profile**
workflow with suite `hudi-benchmark` runs Q8/Q12 on one isolated Ubuntu runner. Supply a full baseline commit
SHA with identical native sources, build inputs and benchmark harness. The runner
admits changes only to the allocation authority, Calc transport and Hudi terminal-batch proof,
builds the two release libraries once, verifies their hashes throughout, and
alternates baseline/candidate then candidate/baseline. Each invocation retains
one warmup and three rotating stock/previous/native trials, four source/sink tasks,
heap state and mini-batching off. It restores candidate sources and deletes runtime
tables even after failure. The workflow artifact retains every observation and log.
This focused comparison is separate from the complete four-configuration matrix.

The same comparison can run on an otherwise idle developer machine using
`python3 bin/bench-hudi-java.py --baseline FULL_BASELINE_SHA --output NEW_DIRECTORY`
with production engine/Parquet libraries in `native/target/release`. Set `JAVA_HOME`
to a supported JDK and prevent system sleep for local timings. The
[processing-time investigation](hudi-processing-time-batches-20261114.json)
retains the local diagnostic and excluded contended attempts.

For complete qualification, select suite `hudi-benchmark` with
`hudi_full_matrix=true` and set `native_run` to a successful focused release
producer. The full mode uses two million events and every unchanged query in four
isolated jobs: heap/RocksDB × mini-batching off/on. Native source/toolchain inputs
must match the producer, its release-build step must have succeeded, and its
library artifact must still be available. Each job downloads those same libraries
and records their hashes. No native rebuild is required for Java-only follow-ups.

`bin/bench-hudi-matrix.py` checks all 198 timing identities plus the exact Q7 stock
planner exclusion per configuration, full-value/count validation, fixed resources,
and actual RocksDB engagement. Partial or duplicate reports fail validation. Source
commit, producer, platform, library hashes and every trial remain in the artifacts.
Generated runtime tables are removed on success and failure. Focused and full
modes have separate workflow concurrency groups; run the required CI, upstream
Flink and docs gates for the final candidate after recording the results.
Each full configuration has a six-hour limit on slower hosted CPUs. The
launcher forwards the existing per-query/engine progress lines to the job console
while retaining the complete byte-for-byte Maven log, including failure output.
The selected queries, warmups, trials, resources and measured boundaries remain
the fixed matrix above; the timeout does not exclude slow controls or partial runs.

## Current hosted qualification (partial)

Both post-optimization two-million-event configurations with **mini-batching on**
have passed: heap and RocksDB. Each retains all 198 timings and the exact Q7
stock planner exclusion. Geometric means of median speedups across 22 timed
queries are:

| State | Stock/native | Previous SF/native | Native medians faster than stock |
| --- | ---: | ---: | ---: |
| memory | 1.27× | 10.28× | 17/22 |
| rocksdb | 1.32× | 8.10× | 17/22 |

The two mini-batching-off configurations are still pending. This is not a
complete matrix or a measured before/after comparison with the earlier heap
runs. Compare engines within each configuration: these are isolated Linux
x86-64 runners with four available processors, about 15.6 GiB physical memory
and JDK 17.0.21; heap ran on AMD EPYC 9V74 and RocksDB on AMD EPYC 9V45.
Their absolute times do not isolate a backend or mini-batch effect.

The fixed configuration retains four Hudi source/sink tasks, eight slots,
8 GiB heap, a 48 GiB logical native off-heap limit, object reuse, one-second
checkpoints, one warmup and three rotating trials per engine. All deterministic
outputs passed full-value multiset comparison; Q12 passed processing-time count
invariants. Both native boundaries, Rust payload footers and final native
resource cleanup passed. RocksDB also passed all three actual backend engagement
checks. [Every observation, range, source/library hash and configuration field](hudi-nexmark-linux-20261114.json) is retained.

Values below are median milliseconds (minimum–maximum). Q5/Q8/Q11/Q13/Q14
retain large stock losses. Q13/Q14 also lose to the Hudi-disabled SF control
in both configurations; Q5 additionally loses to that control on RocksDB.

### memory, minibatching true

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 3,245.7 (3,196.0–3,522.4) | 17,987.1 (17,871.2–18,058.0) | 1,237.4 (1,131.2–1,246.5) | 2.62× |
| q1 | 3,326.9 (3,246.2–3,400.0) | 18,089.1 (18,010.3–18,261.3) | 1,170.7 (1,138.2–1,211.8) | 2.84× |
| q2 | 1,634.6 (1,582.0–1,740.6) | 10,829.8 (10,777.5–10,999.7) | 580.5 (571.5–617.4) | 2.82× |
| q3 | 1,359.3 (1,325.7–1,685.2) | 21,122.3 (21,112.4–21,167.5) | 890.1 (852.9–931.0) | 1.53× |
| q4 | 4,716.9 (4,569.5–5,189.6) | 75,644.5 (75,566.0–77,471.2) | 2,385.7 (2,372.8–2,700.3) | 1.98× |
| q5 | 12,863.9 (12,741.8–13,468.1) | 58,454.2 (57,812.1–58,469.3) | 52,128.0 (51,530.7–52,181.7) | 0.25× |
| q8 | 4,279.9 (4,233.5–4,982.1) | 35,296.4 (35,022.9–35,429.2) | 33,261.6 (33,150.0–33,502.8) | 0.13× |
| q9 | 10,050.4 (9,967.0–10,429.2) | 133,224.3 (131,301.5–137,989.6) | 3,844.0 (3,811.5–4,248.6) | 2.61× |
| q10 | 3,960.9 (3,907.6–3,967.3) | 29,220.4 (29,098.7–29,260.0) | 1,676.6 (1,599.3–1,723.6) | 2.36× |
| q11 | 3,103.6 (2,920.6–3,133.5) | 47,776.2 (47,621.4–47,963.9) | 46,404.4 (46,389.2–46,457.6) | 0.07× |
| q12 | 2,105.8 (2,099.8–2,118.2) | 49,827.6 (49,753.9–49,926.1) | 637.3 (595.3–701.1) | 3.30× |
| q13 | 3,743.8 (3,720.5–3,751.6) | 22,338.6 (22,275.6–22,439.7) | 25,376.0 (25,046.7–25,790.3) | 0.15× |
| q14 | 4,263.2 (4,229.2–4,354.9) | 27,790.7 (26,984.6–28,224.1) | 37,951.6 (36,791.6–38,012.9) | 0.11× |
| q15 | 2,628.6 (2,555.8–2,734.8) | 21,529.1 (21,516.2–21,575.2) | 1,020.6 (952.2–1,066.9) | 2.58× |
| q16 | 3,351.9 (3,121.3–3,947.3) | 30,024.3 (29,856.2–30,794.9) | 1,632.4 (1,541.4–1,651.2) | 2.05× |
| q17 | 3,911.6 (3,667.6–3,990.3) | 29,682.8 (29,638.8–32,739.8) | 2,243.1 (2,201.4–2,487.0) | 1.74× |
| q18 | 13,806.3 (13,787.6–14,715.8) | 138,566.4 (137,447.9–140,900.4) | 10,094.7 (9,230.6–10,534.0) | 1.37× |
| q19 | 52,309.9 (51,474.2–52,650.5) | 91,735.1 (91,362.3–97,184.0) | 6,646.7 (5,456.2–7,051.5) | 7.87× |
| q20 | 4,751.5 (4,729.6–5,129.2) | 96,520.9 (95,411.1–96,699.7) | 1,905.5 (1,821.0–1,986.7) | 2.49× |
| q21 | 2,372.3 (2,342.1–2,374.0) | 19,068.7 (19,051.7–19,410.7) | 1,648.8 (1,638.3–1,828.5) | 1.44× |
| q22 | 4,385.0 (4,226.1–4,785.7) | 18,864.1 (18,810.0–19,111.5) | 1,395.6 (1,374.0–1,433.4) | 3.14× |
| q23 | 16,097.9 (15,431.1–19,668.0) | 84,557.3 (82,873.1–84,918.5) | 4,198.8 (4,092.3–4,607.5) | 3.83× |

Geometric mean: **1.271× stock / 10.277× previous SF**.

### rocksdb, minibatching true

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 2,621.6 (2,584.3–2,946.9) | 13,174.3 (12,745.7–13,528.6) | 1,055.1 (980.5–1,104.9) | 2.48× |
| q1 | 2,796.0 (2,560.1–2,838.1) | 13,717.3 (13,241.5–14,020.1) | 965.0 (895.1–1,031.7) | 2.90× |
| q2 | 1,354.0 (1,265.9–1,365.7) | 8,307.9 (8,232.5–8,318.1) | 611.0 (498.6–668.9) | 2.22× |
| q3 | 1,305.4 (1,295.2–1,543.0) | 16,892.3 (16,788.3–16,953.2) | 873.0 (790.0–965.3) | 1.50× |
| q4 | 9,616.1 (9,216.5–10,854.8) | 62,540.7 (62,158.1–62,833.3) | 3,005.9 (2,928.4–3,334.7) | 3.20× |
| q5 | 17,746.8 (16,879.3–17,902.2) | 117,666.5 (117,368.1–117,828.6) | 143,312.1 (142,981.0–144,223.1) | 0.12× |
| q8 | 4,784.0 (4,610.2–4,903.1) | 30,026.9 (29,868.9–30,895.5) | 29,470.0 (29,171.2–30,247.3) | 0.16× |
| q9 | 19,129.1 (19,127.4–19,205.9) | 101,478.3 (101,381.9–103,916.9) | 4,250.5 (4,046.7–4,681.4) | 4.50× |
| q10 | 3,377.1 (2,955.4–3,400.5) | 20,462.2 (19,909.4–20,640.4) | 1,244.1 (1,237.3–1,267.3) | 2.71× |
| q11 | 7,649.6 (7,443.8–7,729.9) | 65,190.3 (64,530.4–67,393.1) | 64,734.3 (64,046.7–67,745.1) | 0.12× |
| q12 | 1,579.9 (1,555.3–1,933.9) | 36,911.0 (36,484.3–37,066.3) | 616.8 (476.4–622.5) | 2.56× |
| q13 | 3,223.7 (2,805.3–3,224.5) | 14,875.4 (14,697.3–14,883.6) | 16,337.5 (16,197.8–16,431.4) | 0.20× |
| q14 | 3,326.4 (3,219.5–3,341.1) | 17,392.7 (17,153.3–17,506.7) | 23,631.0 (23,462.1–23,838.8) | 0.14× |
| q15 | 2,081.5 (2,049.8–2,163.4) | 14,902.1 (14,895.7–15,177.6) | 1,198.3 (1,122.2–1,368.3) | 1.74× |
| q16 | 3,541.7 (3,181.3–3,968.9) | 22,133.5 (22,060.8–22,554.0) | 2,771.3 (2,534.6–3,389.0) | 1.28× |
| q17 | 3,585.4 (3,356.4–3,638.9) | 21,446.4 (21,416.7–22,147.2) | 2,164.4 (2,101.4–2,474.0) | 1.66× |
| q18 | 15,953.1 (15,563.1–16,039.0) | 102,948.3 (99,612.0–104,160.3) | 9,634.6 (9,505.1–9,775.1) | 1.66× |
| q19 | 36,532.0 (35,720.2–37,800.6) | 68,775.8 (67,532.4–73,665.7) | 5,311.1 (5,221.4–7,302.1) | 6.88× |
| q20 | 9,204.6 (9,041.1–9,777.9) | 72,230.0 (71,890.2–72,306.5) | 2,055.5 (1,825.4–2,304.6) | 4.48× |
| q21 | 1,801.4 (1,637.8–1,984.4) | 14,064.1 (13,378.7–14,271.0) | 1,511.5 (1,210.5–1,630.6) | 1.19× |
| q22 | 3,167.2 (3,085.8–3,372.1) | 13,088.6 (12,830.2–13,314.7) | 996.5 (888.9–1,169.9) | 3.18× |
| q23 | 24,150.2 (24,138.3–25,407.8) | 75,257.8 (74,986.9–75,755.3) | 5,928.8 (5,897.7–6,413.8) | 4.07× |

Geometric mean: **1.321× stock / 8.095× previous SF**.


The first [isolated comparison](hudi-java-comparison-20261114.json) passed on Linux
x86-64 with four available processors, JDK 17.0.21, 200k events, heap state and
mini-batching off. Two alternating baseline/candidate passes give six timed trials
per engine/query/variant. Medians in milliseconds:

| Query | Variant | Stock | Hudi-disabled SF | Native Hudi |
| --- | --- | ---: | ---: | ---: |
| Q8 | Baseline | 1,189.7 | 6,109.3 | 5,832.2 |
| Q8 | Batch proof + owner candidate | 1,312.0 | 6,140.9 | 5,894.3 |
| Q12 | Baseline | 597.1 | 6,251.3 | 6,120.8 |
| Q12 | Batch proof + owner candidate | 633.8 | 6,355.8 | 399.6 |

Q12 improved 15.3× over its native baseline and 1.59× over stock in this candidate.
The Q8 ranges overlap and do not establish an owner-lookup gain; that allocation
candidate was removed. All 72 trials, ranges and source/library hashes are retained.
The subsequent report measures the retained batch proof and Calc transport changes.
These focused results do not qualify the full two-million-event matrix.

The [retained-configuration repeat](hudi-calc-transport-20261114.json) removes the
owner trial and adds Calc input pruning/empty-result suppression. On that runner,
Q8 native medians are 7,824 -> 6,910 ms (1.13×), versus 1,890 ms stock and
7,182 ms Hudi-disabled SF after the change. Q12 is 8,138 -> 529 ms (15.39×),
versus 809 ms stock and 8,157 ms Hudi-disabled SF. It retains another 72 observations
with the same focused workload and alternating order. Compare variants within each
run; the two hosted runners have different absolute timings. Q8's remaining
3.66× stock loss and the full-matrix recheck remain open.

A subsequent local CPU diagnostic of that retained configuration passed six Q8
native iterations against the stock output. Its 11,013 samples place 33.8% of
captured CPU inside Arrow C Data export, including allocation/accounting work.
These inclusive shares overlap, and Calc chains include downstream operators;
they are not isolated component timings or a matched speedup comparison. The
[compact observations](hudi-q8-profile-20261114.json) record source/library hashes
and link the independently verified, versioned archive of folded stacks and logs.
Repeated handoffs remain a target for profiling after the complete matrix.

Historical development logs and diagnostics are preserved in a separate
[versioned evidence archive](https://github.com/jordepic/StreamFusion-contrib/releases/tag/benchmark-evidence-hudi-development-20261114-866620764).
It contains 327 files across multiple source revisions, including interrupted or
withdrawn timings and rejected candidates. Their presence does not qualify those
runs or the current full matrix. The release manifest records every member's size
and SHA-256; the archive and all members were independently downloaded and verified
before removing local raw copies. Retrieve it without running the archived inputs:

```bash
gh release download benchmark-evidence-hudi-development-20261114-866620764 \
  --repo jordepic/StreamFusion-contrib --dir target/hudi-development-evidence
python3 - <<'PY'
from pathlib import Path
import hashlib, json
root = Path('target/hudi-development-evidence')
bundle = json.loads((root / 'manifest.json').read_text())['bundle']
assert hashlib.sha256((root / bundle['path']).read_bytes()).hexdigest() == bundle['sha256']
PY
tar -xzf target/hudi-development-evidence/hudi-development-evidence.tar.gz \
  -C target/hudi-development-evidence
```

The fixture is a COW table partitioned by `sf_partition`, populated once using
stock Hudi with a single ordered input and checkpoints disabled during seeding.
Measured jobs retain one-second checkpoints. Each event retains its original schema and timestamp; the storage
partition is an additional column outside the query projection. Queries retain
their original SQL, four-second watermark and output columns. A wrapper adds a
deterministic storage partition to the output. Source and sink parallelism are
four. The source has four physical partition files. Output keys hash to a
four-way storage partition; low-cardinality query results can occupy fewer than
four directories. The no-op bid query partitions by auction and must
populate all four sink partitions at smoke and representative sizes. Other
append queries use their first output column; updating queries use their first
primary-key column. A hexadecimal MD5 digit selects one of four equally sized
hash buckets. Append results use metadata-free COW files; updating results use version-9
SIMPLE BUCKET MOR with Parquet logs and commit-time ordering. Hudi retains Java
buffering, precombine, index, storage and commits; StreamFusion retains Arrow payloads and
encodes Parquet in Rust.

The default matrix uses heap and RocksDB state, with mini-batching off and on,
two million events, one warmup and three rotating measured trials. The three
engines are stock Flink, StreamFusion with its Hudi boundaries disabled, and
StreamFusion with native Hudi source/sink boundaries. Every native plan must
contain both boundaries and no payload transpose. Every nonempty output must
contain Parquet data with the expected Java or Rust encoder footer.
Encoder checks cover user data and log payloads. Hudi's own timeline/archive and
partition metadata retain their original Java implementation and are excluded.
Object reuse follows the shared Kafka harness: enabled unless `SF_OBJECT_REUSE=false`.
The first q4 warmup in each RocksDB configuration checks that each engine opens
an actual RocksDB database. Watcher setup, checks and teardown are outside
measured trials.

Timing includes planning, admission, reads, shuffles, processing, Java buffering,
Rust encoding and Hudi commits. After the bounded Flink job finishes, the timer
continues until Hudi's original timeline reports no pending commits. This avoids
planning a read before the final coordinator event becomes visible. Empty commits
are enabled for both engines so an empty final checkpoint also reaches a terminal
timeline state. Fixture creation, EXPLAIN and stock-Hudi result
readback are outside timing. The shared test MiniCluster supplies eight slots
and its standard task memory configuration. Resources are identical between
engines. Output comparison sorts full rows, including their storage partition,
with bounded heap and retains
duplicates. Processing-time q12 uses count invariants because its windows depend
on execution time.

```bash
SF_HUDI_BENCH=true SF_ROWS=2000000 SF_WARMUP=1 SF_RUNS=3 \
mvn -B -ntp -Phudi,flink-1.18,bench,hudi-nexmark \
  -pl streamfusion-hudi -am test \
  -Dtest=NexmarkHudiBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1
```

`SF_MATRIX_QUERIES=q0,q1,q4` selects queries. `SF_HUDI_BACKENDS=memory` and
`SF_HUDI_MINIBATCHES=false` select matrix dimensions. `SF_ROWS=2000`, no warmup
and one trial are useful correctness smoke settings, but are insufficient for
performance claims. Structured observations are written to
`streamfusion-hudi/target/hudi-nexmark.json`; override with
`-Dsf.hudi.benchmark.output=...`. Temporary tables and sorted comparison files
are removed after each run.

The completed heap configurations below are the baseline before persistent-window
deadline caching. The first RocksDB campaign stopped during Q5 to prioritize its
performance regression. The full matrix will be requalified after focused tuning.
macOS power logs confirmed repeated host
suspensions during the earlier campaign, and a later previous-path Q10 run failed
with an expired Hudi commit heartbeat. The earlier full-value comparisons remain
correctness evidence; their timings are retained as diagnostics and do not qualify
speedups: [Q0](hudi-nexmark-q0-20261113.json),
[Q1](hudi-nexmark-q1-20261113.json), and [Q4](hudi-nexmark-q4-20261113.json).

Prevent system sleep throughout measurement. On macOS, prefix Maven with
`caffeinate -is`; this assertion lasts only for that command and leaves the display
policy unchanged. Each measured observation records its UTC start, monotonic
elapsed time and wall-clock elapsed time. A clock gap of one second or more fails
the run. The power guard is still necessary because host suspension also affects
Hudi heartbeats and processing-time timers.

For focused CPU profiling, use `SF_HUDI_BENCH=true SF_HUDI_PROFILE=true` with
`SF_MATRIX_QUERIES` selecting one deterministic query and one state/mini-batch
configuration. Select `-Dtest=NexmarkHudiBenchmark#profileNativePipeline` with the
same release profile. This mode computes a stock reference once, then repeats
the native pipeline for `SF_WARMUP + SF_RUNS` iterations, checking full values,
encoders and final native cleanup. It reports `profiled`, so diagnostic captures
cannot be mistaken for a passed comparison matrix. It avoids repeating the slow
previous engine while investigating an admitted native path; normal qualification
continues to compare all three engines.

Stock Flink 1.18.1 rejects the unchanged q7 query over the Kafka corpus's
TIMESTAMP_LTZ rowtime when it compares that time with a TIMESTAMP window boundary.
The matrix records the exact planner rejection for this query and configuration;
it supplies no timing or parity claim for q7. The original schema and SQL remain
unchanged. Other planner errors fail the run.

## Baseline heap configurations

Both heap-state configurations passed on an Apple M4 (10 available processors,
16 GiB physical memory), JDK 17.0.17, with 8 GiB maximum heap and the shared test
cluster's 48 GiB logical native off-heap limit. The latter is a configured limit,
not reserved physical RAM. These short complete-job times include startup and
commits and do not establish sustained streaming throughput. Compare engines
within this report, not with the README's Kafka results from another machine.

[Structured observations, CPU profile summary and memory context](hudi-nexmark-20261114.json)
retain all 396 timed observations, both stock q7 rejections and the full configuration.
The overall report is marked `partial`: the two RocksDB configurations are incomplete.
All 21 deterministic queries passed full-value multiset comparison
in each completed configuration; q12 passed processing-time count invariants.
Payload encoders and final native resource cleanup passed. The largest
wall/monotonic clock discrepancy was 14.0 ms.

Geometric means of median speedups across all 22 timed queries, including
processing-time q12, are **0.55× stock / 7.51× previous StreamFusion** with
mini-batching off and **0.64× / 8.78×** with it on. Twelve and fifteen native
medians respectively beat stock; several differences are small relative to their
ranges. Q5/Q8/Q11/Q12 and the lookup/UDF paths remain substantially slower.
This matrix does not qualify every admitted pipeline as accelerated. The draft's
performance disposition remains open in issue #663.

The first configuration ran at `0037e0bfa`; the second ran with the `197d45bfd`
MOR reader fix, which preserves physical field layouts. The measured COW source,
append/MOR sinks, engine, codecs and harness are unchanged between these revisions.
The subsequent persistent-window deadline optimization has separate measurements
below and is not included in these baseline tables.

Values below are median milliseconds with minimum–maximum across three measured
trials per engine, following one warmup. A ratio below one means native is slower.

### Mini-batching off

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 986.1 (957.0–1,053.7) | 7,143.7 (7,140.2–7,368.9) | 529.9 (528.8–591.4) | 1.86× |
| q1 | 1,039.4 (974.4–1,069.8) | 7,317.8 (7,240.8–7,323.0) | 584.0 (575.7–587.9) | 1.78× |
| q2 | 597.5 (548.8–640.7) | 5,486.2 (5,417.6–5,490.3) | 364.9 (346.8–390.0) | 1.64× |
| q3 | 554.3 (553.4–557.7) | 15,393.6 (15,342.6–15,447.2) | 437.5 (430.1–452.3) | 1.27× |
| q4 | 2,039.6 (1,976.5–2,504.2) | 85,742.2 (83,596.4–88,342.1) | 1,323.7 (1,145.9–1,527.8) | 1.54× |
| q5 | 3,483.4 (3,419.2–3,588.3) | 32,159.3 (31,799.2–32,327.2) | 35,211.0 (34,940.9–35,375.9) | 0.10× |
| q8 | 1,296.4 (1,288.7–1,322.6) | 23,829.9 (23,444.2–24,234.6) | 29,411.0 (29,283.4–29,496.0) | 0.04× |
| q9 | 2,741.7 (2,676.7–3,021.2) | 166,255.0 (165,392.0–177,349.0) | 2,804.8 (2,769.9–2,835.9) | 0.98× |
| q10 | 1,217.2 (1,168.0–1,218.9) | 10,938.9 (10,845.9–11,013.2) | 671.5 (632.3–673.0) | 1.81× |
| q11 | 831.2 (809.1–842.4) | 30,535.2 (30,214.2–30,575.7) | 29,805.1 (29,661.2–30,026.8) | 0.03× |
| q12 | 563.2 (561.9–649.6) | 31,973.8 (31,885.9–32,042.1) | 31,553.6 (31,368.1–31,590.5) | 0.02× |
| q13 | 1,148.5 (1,088.8–1,285.0) | 9,573.9 (9,459.2–9,646.7) | 11,289.3 (10,970.3–11,460.1) | 0.10× |
| q14 | 1,205.5 (1,179.8–1,206.7) | 9,204.0 (9,158.5–9,421.2) | 13,565.1 (13,283.7–13,674.3) | 0.09× |
| q15 | 7,668.2 (7,604.7–7,719.7) | 27,506.3 (27,276.4–27,623.0) | 7,636.2 (7,566.7–7,707.1) | 1.00× |
| q16 | 4,705.6 (4,675.2–5,052.5) | 34,121.6 (34,109.0–34,804.2) | 5,310.6 (4,815.8–5,443.8) | 0.89× |
| q17 | 3,233.4 (2,921.8–3,364.2) | 35,431.1 (35,420.2–35,825.6) | 3,082.6 (2,788.6–3,210.8) | 1.05× |
| q18 | 4,052.6 (3,668.2–4,104.2) | 69,200.0 (67,687.0–69,340.2) | 5,231.2 (3,662.4–6,703.8) | 0.77× |
| q19 | 11,287.3 (10,884.4–14,690.8) | 59,540.8 (59,081.0–60,001.1) | 10,531.0 (9,826.4–13,355.9) | 1.07× |
| q20 | 1,527.6 (1,165.6–2,292.3) | 59,850.4 (59,053.6–59,988.7) | 1,013.6 (911.0–1,229.4) | 1.51× |
| q21 | 650.8 (650.5–853.5) | 8,749.3 (8,337.6–8,797.6) | 655.7 (645.6–878.6) | 0.99× |
| q22 | 1,308.4 (1,307.2–1,469.6) | 7,449.0 (7,411.2–7,678.1) | 623.0 (603.6–672.8) | 2.10× |
| q23 | 3,943.4 (3,744.8–4,016.1) | 62,203.8 (62,017.2–62,734.7) | 1,556.3 (1,544.9–1,621.5) | 2.53× |

Geometric mean: 0.554× stock, 7.513× previous SF.

### Mini-batching on

| Query | Stock ms (range) | Previous SF ms (range) | Native ms (range) | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| q0 | 1,039.5 (997.8–1,073.7) | 7,398.4 (7,370.4–7,570.4) | 547.2 (538.0–548.6) | 1.90× |
| q1 | 1,086.1 (961.0–1,102.1) | 7,428.2 (7,426.5–7,638.0) | 566.3 (533.1–626.0) | 1.92× |
| q2 | 620.3 (568.8–636.4) | 5,634.0 (5,604.0–5,658.4) | 372.3 (346.3–404.9) | 1.67× |
| q3 | 565.0 (547.1–646.7) | 15,770.6 (15,739.5–15,977.6) | 436.2 (433.7–452.0) | 1.30× |
| q4 | 1,398.6 (1,269.7–1,578.2) | 50,070.6 (49,961.0–50,207.1) | 963.9 (950.4–1,175.0) | 1.45× |
| q5 | 3,829.0 (3,725.8–3,830.6) | 31,797.2 (31,794.1–32,089.4) | 35,934.2 (35,902.1–35,950.7) | 0.11× |
| q8 | 1,255.3 (1,241.9–1,380.5) | 21,951.2 (21,866.9–22,018.9) | 26,896.0 (26,870.4–26,949.7) | 0.05× |
| q9 | 2,852.9 (2,696.6–3,045.9) | 76,686.1 (76,227.9–80,805.1) | 1,596.6 (1,429.4–1,752.8) | 1.79× |
| q10 | 1,243.5 (1,225.4–1,324.6) | 11,471.6 (11,459.6–11,555.2) | 638.1 (628.2–645.7) | 1.95× |
| q11 | 848.7 (844.8–856.5) | 31,966.4 (31,958.6–32,190.8) | 31,290.2 (31,177.1–31,353.3) | 0.03× |
| q12 | 603.2 (578.9–610.8) | 33,661.3 (33,352.7–33,930.0) | 32,895.9 (32,874.9–33,038.3) | 0.02× |
| q13 | 1,099.7 (1,027.7–1,721.7) | 9,793.7 (9,792.4–9,872.7) | 11,475.1 (11,468.4–11,531.8) | 0.10× |
| q14 | 1,199.2 (1,182.4–1,217.5) | 9,510.1 (9,453.5–9,529.7) | 13,681.0 (13,645.7–13,765.6) | 0.09× |
| q15 | 850.5 (828.5–925.8) | 11,004.8 (10,975.9–11,042.8) | 448.8 (433.2–449.8) | 1.89× |
| q16 | 1,083.7 (1,008.2–1,118.3) | 14,990.1 (14,884.4–15,321.6) | 711.9 (677.0–730.5) | 1.52× |
| q17 | 1,302.5 (1,220.3–1,464.9) | 12,394.1 (12,031.0–12,602.9) | 1,146.6 (983.2–1,164.6) | 1.14× |
| q18 | 4,122.0 (3,933.7–5,051.2) | 65,727.5 (65,463.8–78,558.7) | 4,483.1 (3,817.4–6,261.9) | 0.92× |
| q19 | 11,508.4 (11,187.7–21,950.5) | 45,982.9 (45,940.6–46,601.5) | 2,867.4 (2,020.2–2,937.9) | 4.01× |
| q20 | 1,463.0 (1,371.6–1,756.4) | 64,212.6 (62,649.6–65,996.5) | 1,033.8 (1,005.8–1,085.5) | 1.42× |
| q21 | 704.4 (689.8–707.2) | 8,677.6 (8,667.4–8,689.9) | 676.2 (665.4–684.5) | 1.04× |
| q22 | 1,341.1 (1,306.6–1,385.4) | 7,742.0 (7,630.0–7,749.3) | 677.9 (571.5–687.2) | 1.98× |
| q23 | 3,616.1 (3,581.9–3,939.3) | 47,349.2 (47,209.3–47,472.6) | 1,931.0 (1,849.8–2,076.9) | 1.87× |

Geometric mean: 0.640× stock, 8.780× previous SF.

## Persistent-window deadline optimization

A focused Q5 RocksDB CPU capture attributed 71.4% of 19,743 samples to aggregate
firing and 14.8% to window-join firing. The shared state implementation now uses
the earliest pending end to avoid firing scans before any window is due. Inserts
lower that bound; restore and timer import rebuild it. Watermark positions,
late-row decisions, payload representations and checkpoint formats are unchanged.
Regional calendar firing retains its existing path.

The unchanged Q5 query at 200k events, four source/sink tasks, RocksDB and
mini-batching off compares the preserved old library with the updated release
library. Each uses one warmup and three rotating trials per engine, full-value
readback, encoder checks and final native cleanup.

| Engine | Before median ms (range) | After median ms (range) |
| --- | ---: | ---: |
| Stock | 1,390.3 (1,345.6–1,565.7) | 1,396.5 (1,246.9–1,643.3) |
| Previous SF boundaries | 25,769.0 (24,502.9–30,935.4) | 7,648.0 (7,604.6–8,504.8) |
| Native Hudi boundaries | 35,801.2 (35,071.8–41,816.8) | 10,118.9 (8,845.8–10,900.4) |

The native median improves **3.54×**, but remains **7.25× slower than stock**.
The previous-boundary path also benefits because the optimization is shared engine
state code. [All 18 timings, source trees, library hashes and CPU diagnostic](hudi-window-firing-20261114.json)
are retained. A separate smoke checks actual RocksDB databases for all three engines.
All 99 affected native window/store tests pass, including canonical state transfer.
These smaller complete jobs do not establish the two-million-event result or
sustained throughput; those checks and full-matrix qualification remain pending.

The subsequent cursor revision visits populated key groups through one iterator,
seeks from the earliest live end, and skips accumulator namespaces when loading
timers. A repeated deadline-cache baseline has medians of 1,289 ms stock,
7,635 ms Hudi-disabled SF and 10,039 ms native. The cursor revision records
1,267 / 7,593 / 8,004 ms respectively. Native ranges are 8,528–10,479 ms before
and 7,895–8,026 ms after: a 1.25× median improvement. The combined improvement
from the original 35,801 ms median is 4.47×, while stock is still 6.32× faster.
All 100 affected native tests pass, including negative window ends, keyless
timers and frozen read views. The same structured report now retains all 54
timings across retained, intermediate and rejected variants. Per-timer dirty
tracking was rejected after overlapping native ranges and a regression in the
Hudi-disabled path; the first per-group-only seek variant was also inconclusive.

## Remaining bottleneck

[Issue #663](https://github.com/datafusion-contrib/StreamFusion/issues/663)
tracks the remaining eager-watermark, handoff and allocation work.

An independent Q8 release CPU profile collected 4,163 samples. Inclusive shares
were 60.2% under watermark assignment, 62.7% under Calc, 16.2% under local window
aggregation and 13.7% under Arrow memory reservation; the shared Arrow owner
lookup accounted for 8.3%. These shares overlap and are not independent costs or
elapsed-time improvements. The profile identifies eager fragmentation, repeated
handoffs and allocation as follow-up work; it does not establish all causes of
the regression. The 29.4-second native median also loses to the previous path's
23.8 seconds, so the remaining cost cannot all be attributed to pre-existing core
operators. Windows retain their observable watermark boundaries.

### Host memory context

Two read-only OS/process snapshots during heap/mini-batching-on Q23's previous
engine trial recorded benchmark JVM RSS of about 6.6–6.4 GiB and about 1.8 GiB
of global swap usage. Global compression/decompression counters advanced, while
swap-in/out counters did not change during the five-second sample. These global
counters are not process-specific, and the JVM runs all three engines; this
sample does not establish the cause of any regression. It records the host's
memory conditions without excluding unfavorable observations. Future matched
profiles should measure memory pressure as well as CPU and allocation.
