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

The complete expanded matrix is under validation. The two-million-event Q0
comparison below completed one warmup and three rotating measured trials on
JDK 17.0.17, aarch64, with four source/sink tasks, heap state, mini-batching off,
object reuse on, an 8 GiB heap and the shared 48 GiB task off-heap budget.
All trials verified the full multiset of 1,840,000 output bids, all four sink
partitions and the expected encoder footer. The harness separately checks native
resource cleanup when the complete matrix finishes.

| Engine | Median ms | Range ms |
| --- | ---: | ---: |
| Stock Flink | 1,080.5 | 1,007.0–2,011.6 |
| Previous StreamFusion Hudi boundaries | 7,884.6 | 7,873.4–9,970.4 |
| Native Hudi boundaries | 626.6 | 589.5–639.3 |

For this configuration the native median is 1.72× faster than stock and 12.58×
faster than the previous partitioned-table behavior. These are complete bounded
job times, including commit visibility. The subsecond native times still include
startup and do not establish steady-state throughput. The stock range also
shows substantial variability. [All Q0 observations](hudi-nexmark-q0-20261113.json)
retain every measured trial. This result does not qualify the remaining queries
or state configurations; the small smoke runs establish correctness only.

The same run completed Q1 with medians of 2,200.5 ms stock, 11,077.0 ms previous
StreamFusion and 641.7 ms native (ranges 1,461.0–3,453.8, 9,283.4–11,996.4
and 584.0–766.0 ms). Every trial compared all 1,840,000 output rows. Its
[Q1 observations](hudi-nexmark-q1-20261113.json) retain the matching configuration
and source/harness identity. This is a 3.43× median gain over stock in this
bounded configuration; the wide stock range remains visible.

Q4 completed the same two-million-event comparison across all four state and
mini-batch configurations after the terminal watermark rewrite was extended
through its regular join and non-windowed aggregates. All 36 measured results
compared the complete 100-row output across four sink partitions. The six
RocksDB warmups verified actual database creation. Final handle and reservation
checks passed. Values below are median milliseconds (minimum–maximum).

| State | Mini-batch | Stock | Previous | Native | Stock/native |
| --- | --- | ---: | ---: | ---: | ---: |
| memory | false | 4,824.1 (2,126.3–5,668.8) | 142,394.1 (86,414.8–234,831.6) | 2,683.9 (1,458.1–2,701.3) | 1.80× |
| memory | true | 1,591.1 (1,419.8–1,809.8) | 49,789.4 (49,729.1–52,572.8) | 1,026.3 (898.2–1,202.4) | 1.55× |
| rocksdb | false | 24,952.8 (6,566.1–29,171.8) | 83,838.4 (83,296.0–84,330.4) | 3,657.0 (3,373.5–3,686.5) | 6.82× |
| rocksdb | true | 5,338.0 (4,553.7–7,394.6) | 52,620.7 (51,388.0–54,362.1) | 1,578.6 (1,519.4–1,653.0) | 3.38× |

[All Q4 observations](hudi-nexmark-q4-20261113.json) retain every trial and
resource setting. Stock RocksDB and the previous heap path have wide ranges;
these bounded-job results do not establish sustained throughput. Q0/Q1 used
source base `93077f8fb`; Q4 used the rebased `07fd1f0d4` native sources. Other
queries remain under qualification.

Stock Flink 1.18.1 rejects the unchanged q7 query over the Kafka corpus's
TIMESTAMP_LTZ rowtime when it compares that time with a TIMESTAMP window boundary.
The matrix records the exact planner rejection for this query and configuration;
it supplies no timing or parity claim for q7. The original schema and SQL remain
unchanged. Other planner errors fail the run.
