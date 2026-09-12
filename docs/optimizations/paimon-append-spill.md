# Paimon append spill stays columnar

Paimon's automatic writer-count transition used to send a native append sink back through Java
row serialization and Parquet encoding. StreamFusion now retains Arrow batches and spills them
as compressed IPC streams, then drains one bucket at a time through its native Parquet writer.
Finishing and retaining existing data files at the transition also avoids decoding and rewriting
them. The Java connector still owns the surrounding commit and compaction lifecycle.

This targets tasks with many active table-partition/bucket writers. The existing Kafka Nexmark
Paimon sink diagnostic uses unpartitioned tables, so it does not exercise the default threshold
of 10 writers. Its published numbers are unchanged.

`PaimonAppendSpillBenchmark` compares the previous native bundle path with stock spilling and
the new native spill buffer. One task writes 131,072 rows with the nested
Paimon parity schema into 16 table partitions and four buckets per partition, using a 1 MiB write
buffer. There is one warmup and three measured runs, rotating engine order; results use the best
time. The timer covers ingestion, Arrow conversion on both native paths, spill, file encoding,
commit, and close. Input generation, native-path routing preparation, and readback are outside
the timer, so this is a sink diagnostic rather than an end-to-end SQL benchmark. Every run's rows
are compared with the previous path, and native Parquet footers are required on the new path.
The separate SQL and writer parity tests compare with stock Paimon row ingestion.

On the release build, the previous path took **1.514 s** and native Arrow spilling took
**1.353 s**: **1.12× throughput** on this workload. This measures the transition and spill path;
it is not a speedup claim for sinks that stay below the writer-count threshold.

```bash
SF_PAIMON_SPILL_BENCHMARK=true mvn test -Pbench,paimon \
  -pl :streamfusion-paimon -am -Dtest=PaimonAppendSpillBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.testForks=1
```

See [Paimon coverage](../connectors/paimon.md#append-buffering-and-local-spill) for the memory and
disk limits, supported codecs, and file-layout differences at the transition.
