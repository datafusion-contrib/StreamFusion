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

Timing qualification is being rerun. macOS power logs confirmed repeated host
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
