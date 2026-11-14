# Four-partition Hudi Nexmark

`NexmarkHudiBenchmark` uses the existing Nexmark matrix queries and Kafka event
corpus with Hudi tables at both ends. This is a separate storage workload from
the README Kafka benchmark. It requires the released Hudi 1.2.1 Flink 1.18 bundle
and release StreamFusion engine/Parquet libraries.

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

The remaining configurations are undergoing qualification. macOS power logs confirmed repeated host
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

## Completed heap configurations

Both heap-state configurations passed on an Apple M4 (10 available processors,
16 GiB physical memory), JDK 17.0.17, with 8 GiB maximum heap and the shared test
cluster's 48 GiB logical native off-heap limit. The latter is a configured limit,
not reserved physical RAM. These short complete-job times include startup and
commits and do not establish sustained streaming throughput. Compare engines
within this report, not with the README's Kafka results from another machine.

[Structured observations, CPU profile summary and memory context](hudi-nexmark-20261114.json)
retain all 396 timed observations, both stock q7 rejections and the full configuration.
The overall report is marked `partial`: the two RocksDB configurations remain
in progress. All 21 deterministic queries passed full-value multiset comparison
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
Remaining configurations use that reader fix and record their own source revisions.

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

### Remaining bottleneck

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
