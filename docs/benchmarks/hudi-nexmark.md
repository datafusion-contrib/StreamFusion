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

## Current qualification: heap state, mini-batching off

The completed configuration passed on an Apple M4 (10 available processors,
16 GiB physical memory), JDK 17.0.17, with 8 GiB maximum heap and the shared test
cluster's 48 GiB logical native off-heap limit. The latter is a configured limit,
not reserved physical RAM. These short complete-job times include startup and
commits and do not establish sustained streaming throughput. Compare engines
within this report, not with the README's Kafka results from another machine.

[Structured observations and CPU profile summary](hudi-nexmark-20261114.json)
retain all 198 timed observations, the stock q7 rejection and the full configuration.
The overall report is marked `partial`: only this configuration is complete.
All 21 deterministic queries passed full-value multiset comparison; q12 passed
processing-time count invariants. Payload encoders and final native resource
cleanup passed. The largest wall/monotonic clock discrepancy was 14.0 ms.

The geometric mean of median speedups is **0.55× stock** and **7.51× previous
StreamFusion** across all 22 timed queries, including processing-time q12.
Twelve native medians beat stock; several differences are small relative to their
ranges. Q5/Q8/Q11/Q12 and the lookup/UDF paths remain substantially slower.
This matrix does not qualify every admitted pipeline as accelerated. The other
three backend/mini-batch configurations are still running, and the draft's
performance disposition remains open.

The completed configuration ran at `0037e0bfa`. The subsequent `197d45bfd` fix
preserves physical field layouts in the MOR reader; the measured COW source,
append/MOR sinks, engine, codecs and harness are unchanged. Remaining configurations
run with that reader fix and record their own source revisions.

Values below are median milliseconds with minimum–maximum across three measured
trials per engine, following one warmup. A ratio below one means native is slower.

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
