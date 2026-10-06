# Apache Fluss

**Status:** Experimental, opt-in. The optional `streamfusion-fluss` module uses the
released Apache Fluss Java connector 1.0.0. Enable its verified planner substitutions
with `-Dstreamfusion.fluss.enabled=true`. The supported contracts have integration coverage, but this experimental module keeps
the stock connector as the default.

The integration uses the existing Java connection for metadata, security, routing and
broker RPCs. It reads schema-less Arrow IPC messages from `FetchLog`, preserving the
primary-key log's per-row change-type sidecar, and hands owned Arrow buffers to the
existing StreamFusion Java-to-Rust bridge. Compatible vector layouts share buffers;
timestamp input is converted to StreamFusion's component representation. Millisecond
input retains the received millis data and parent validity buffers, adding a zero
fractional-nanos buffer and a small all-valid child bitmap without rebuilding timestamps row by row.
Millisecond timestamp output shares the existing millis and validity buffers. It does not depend
on fluss-rust or require broker changes. The data plane uses version-specific Java
RPC implementation APIs, pinned to connector 1.0.0; this is not a stable public Arrow
polling contract. [Issue #25](https://github.com/datafusion-contrib/StreamFusion/issues/25)
tracks the future public-API migration.

Direct RPC receive allocations are retained through Arrow's foreign-allocation API and
charged to its allocator until the last vector reference closes. IPC metadata is parsed
separately, and the body buffers reference the received allocation. Uncompressed bodies
avoid a network-buffer-to-Arrow body copy; compressed bodies still need decompression
allocations. Non-direct buffers and remote files use the copying decoder. Rust may copy
under-aligned buffers during import, and high-precision timestamp conversion also allocates;
the complete path is not claimed to be zero copy. Append serialization uses one exactly
sized wire byte array, avoiding growing streams and their final array copy; the released Java request encoder still copies it into the outbound RPC buffer.
Fluss's memory-segment zero-copy send path is used for server responses, not client requests.

## Current implementation boundary

The source admits streaming, non-partitioned `ARROW` log tables, including primary-key
logs, with earliest, latest or timestamp startup (append-only `full` startup is also an earliest log scan) and the upstream enumerator's bounded
stopping offsets. Nonempty top-level projections on schema-version-1 tables are sent to the broker.
Fluss 1.0 schema evolution adds nullable trailing columns. On evolved tables, the source
reads complete batches and selects columns locally, filling historical missing columns
with nulls; this avoids positional projection of a new column from an older batch. A projected
struct retains its complete nested fields. CDC kinds remain aligned with the selected
rows. Repeated identical scans can share a columnar source, preserving their shared input schema.
Checkpoint positions advance after downstream collection succeeds; restoring a
position inside a log batch slices away the already-consumed rows.

Primary-key initial snapshot startup, partition discovery, data-lake hybrid reads, merge engines, pushed filters, limits,
aggregate pushdowns, custom client settings, empty projections and unsupported watermarks use the stock Flink
source. Tiered logs use Fluss's existing Java downloader and then select columns locally;
selective remote range reads are not implemented.

The Arrow sink is limited to insert-only input and explicitly resolved `ARROW` append-only tables without partition
or bucket keys. Input fields map positionally to destination columns. Production uses
`ProduceLog` with the table's Arrow compression setting: matching that setting is needed
because broker projection reconstructs IPC compression metadata from table configuration.
The writer encodes on the task thread and pipelines a bounded queue across independent
buckets. Requests to each bucket remain sequential, including retries, to preserve batch
sequence order. Queued batches for one bucket can share a request through a composite
byte view, with at most five batches and the smaller of 1 MiB and the configured request-size limit.
Larger batches use individual requests; grouping wide batches into multi-megabyte
messages regressed the uncompressed transport benchmark. Five matches
the released broker's deduplication history, so retrying a complete grouped request does
not replay batches that have already fallen out of that history. Larger input roots are
split into columnar slices toward the writer's batch size; a single oversized record
still fails like the stock writer. Checkpoint and end-of-input flushes wait for every acknowledgement.
The queue obeys the Java client's writer-buffer limit and a 64-batch ceiling; production
queue reservations are charged to the shared TaskManager memory budget. Primary-key production,
batch sinks, explicit undo-recovery identities, bucket/dynamic partition shuffle,
merge engines, lake writes, row modifications and sink materialization use the stock
Flink sink.

Append delivery matches the released connector's default at-least-once recovery contract.
Writer IDs and per-bucket sequences deduplicate retries within one writer lifetime, and
checkpoint flush waits for acknowledgements. Replaying data after a job restart can append
duplicates; this path does not implement a transactional or checkpoint-aware undo protocol.
The Kafka README suite uses transactional exactly-once output, so cross-connector timing
comparisons must state this difference rather than imply equal delivery guarantees.

The admission whitelist and integration tests cover the contracts described above;
other combinations retain the stock connector. Isolated transport measurements below
establish improvements within their measured boundaries. These results do not establish
uniform end-to-end speedups for every query or deployment.

## Build and validation

Install Flink's normal `fluss-flink-2.2:1.0.0` connector alongside `streamfusion-core`
and `streamfusion-fluss`. The optional integration adds no Rust library and reuses the
core's Arrow handoff. Fluss classes are excluded from the core artifact. Shared-source
Javadoc generation resolves the released Fluss API as a documentation-only dependency,
following the other optional connectors; it does not add Fluss to the core consumer POM.

Compilation and Arrow schema/framing/ownership/admission contracts also pass with
Flink 1.18. The full end-to-end benchmark and packaging validation use Flink 2.2.

The module's Docker integration fixture runs released Fluss 1.0.0 and ZooKeeper 3.9.2.
The Fluss brokers use a 1 GiB JVM heap and a 512 MiB direct-memory
limit. Fluss also requires its coordinator and ZooKeeper; their overhead remains part
of the topology and is not claimed to be identical to Kafka's:

```sh
mvn -pl streamfusion-fluss -am test \
  -Dtest=FlussArrowClientTest,FlussArrowLogBatchTest,FlussArrowSchemaTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

These tests exercise official-client interoperability, projected primary-key changelogs,
resuming within an append batch, reader checkpoint/restore with queued batches, failed
downstream collection, historical nullable columns, paused-broker append flush,
splitting large Arrow roots at the configured request limit, repeated grouped-request
deduplication, wire checksums and shared-buffer lifetime. They are
correctness checks, not performance measurements.

## Isolated transport profiling

```sh
SF_FLUSS_TRANSPORT_BENCH=true mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=FlussTransportBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dstreamfusion.fluss.profile=true \
  -Dfluss.transport.batchRows=8192 -Dfluss.transport.batches=128 -Dfluss.transport.runs=3
```

This fixture uses a released single-tablet broker, one bucket, two nullable BIGINT columns,
one warmup and repeated trials for NONE, LZ4_FRAME and ZSTD compression. Connections,
table creation, fixture generation and teardown are outside timing. Append timing retains
serialization, compression, routing, RPC and acknowledgement for every batch. Read timing
retains broker fetch, framing, decode, vector ownership and a checksum of the first column;
the stock reader computes the same checksum through row polling. It excludes Flink,
JNI and query operators, so it cannot establish full-job performance or a Kafka speedup.
Fetch replies ending in a partial batch resume at the last complete batch's offset.
The fixture also times the released stock Java append writer using prebuilt rows,
including its buffering, serialization and final acknowledgement flush. A second
table verifies its checksum outside timing. `-Dfluss.transport.wide=true` adds a
128-character string, millisecond timestamp and decimal, with independent null patterns;
`-Dfluss.transport.async=true` exercises the production acknowledgement pipeline.
Outside timing, a warm fetched batch is also round-tripped through the production JNI
bridge. `FLUSS_HANDOFF` reports buffer bytes whose addresses remain shared and whose
addresses change, and verifies every returned value. Address changes identify ownership
or alignment work; this metric is not a general count of copied bytes. The fixture's
reader now requests StreamFusion's production timestamp component layout, and the
writer receives that same layout; these conversions remain inside transport timing.
Earlier wide measurements below used the wire timestamp layout and exclude that
conversion. The current handoff check counts nested component buffers recursively.
The optional profile switch reports serialization/compression, acknowledged produce RPC,
fetch RPC and decode/ownership durations separately. It is disabled in normal execution.
The produce-request count includes retries and distinguishes request coalescing from
faster serialization of the same number of requests.
For a controlled comparison with the previous pipeline, set
`-Dstreamfusion.fluss.coalesce.enabled=false`; this diagnostic switch disables joining
queued batches into a request without changing encoding, slicing or acknowledgement.
`-Dstreamfusion.fluss.millis-sharing.enabled=false` restores the previous rowwise
millisecond timestamp input conversion for a controlled layout comparison.

The initial copying implementation measured these milliseconds for 1,048,576 rows on the
development machine (three trials, release profile, with JFR profiling enabled):

| Compression | Acknowledged Arrow append | Arrow fetch/decode | Stock Java row fetch |
| --- | --- | --- | --- |
| NONE | 173.719 / 115.782 / 94.891 | 64.249 / 41.479 / 48.606 | 118.573 / 107.845 / 109.337 |
| LZ4_FRAME | 119.865 / 141.752 / 122.959 | 46.448 / 45.212 / 43.626 | 94.244 / 87.903 / 93.503 |
| ZSTD | 135.384 / 130.868 / 131.151 | 27.613 / 24.983 / 23.687 | 78.842 / 81.883 / 68.122 |

These are baseline measurements before receive-buffer retention and exactly sized append
serialization. JFR heap allocation samples identified growing byte streams and their final
array copies as producer costs; sampled allocation weights are not copied-byte counts.

After receive-buffer retention and exactly sized serialization,
the same fixture measured the following milliseconds. Profiling counters were enabled;
all trial checksums matched the stock reader.

| Compression | Acknowledged Arrow append | Arrow fetch/decode | Stock Java row fetch |
| --- | --- | --- | --- |
| NONE | 130.581 / 100.550 / 83.531 | 62.139 / 38.891 / 43.232 | 111.354 / 104.720 / 135.006 |
| LZ4_FRAME | 114.449 / 110.522 / 106.581 | 45.522 / 45.001 / 39.632 | 90.030 / 136.708 / 83.410 |
| ZSTD | 119.923 / 116.550 / 107.761 | 28.709 / 25.949 / 23.852 | 72.615 / 71.343 / 69.164 |

The median component durations identify the remaining costs:

| Compression | Encode/compress | Acknowledged produce RPC | Fetch RPC | Decode/ownership |
| --- | ---: | ---: | ---: | ---: |
| NONE | 9.810 | 89.499 | 34.582 | 7.089 |
| LZ4_FRAME | 37.376 | 71.445 | 15.235 | 26.491 |
| ZSTD | 50.155 | 65.128 | 6.923 | 18.298 |

Component medians are independent and do not sum exactly to the median trial duration.
RPC time dominates uncompressed append; compression and decompression remain material
for the compressed modes. These small fixed-width batches are transport diagnostics,
not representative Nexmark headline results. The
measurements above precede bounded acknowledgement pipelining.

With pipelining enabled, the isolated ZSTD append fixture measured
66.704 / 57.219 / 62.394 ms, versus 119.923 / 116.550 / 107.761 ms for sequential
acknowledgements. Encoding and RPC durations overlap in the pipelined path.
End-to-end duration includes Java client teardown. Released Fluss 1.0 uses Netty's
default graceful event-loop shutdown for each connection; source and sink teardown can
therefore dominate short jobs. The isolated transport fixture excludes that teardown
and is reported separately rather than subtracted from job durations.

The wider fixture was then measured with 1,048,576 rows, batches of 8,192, asynchronous
acknowledgements, one warmup and three trials using the worktree's release build. Its
prebuilt input contains two BIGINTs, a 128-character string, a millisecond timestamp
and DECIMAL(18,3), with nullable string, timestamp and decimal values. Milliseconds:

| Compression | Arrow append | Stock Java append | Arrow read | Stock Java read |
| --- | --- | --- | --- | --- |
| NONE | 342.339 / 309.518 / 251.579 | 296.559 / 2302.171 / 258.526 | 325.077 / 250.203 / 183.806 | 509.313 / 442.982 / 360.554 |
| LZ4_FRAME | 277.284 / 263.920 / 260.015 | 437.020 / 421.786 / 418.525 | 205.704 / 191.920 / 185.501 | 326.823 / 331.868 / 307.326 |
| ZSTD | 230.621 / 228.045 / 266.841 | 331.756 / 365.448 / 456.675 | 254.891 / 280.214 / 282.637 | 348.604 / 392.360 / 423.462 |

Compressed production improves on the stock writer in this fixture; the uncompressed
median is slightly slower and has a large stock-writer outlier. The stock writer produced
73 / 62 / 87 log batches for NONE / LZ4 / ZSTD respectively, versus 128 Arrow batches.
This identifies request batching as a producer optimization to investigate. These remain
isolated transport measurements, not full-job or Kafka performance claims.

## Bounded transport measurements

The final transport runs use 1,048,576 rows, 8,192-row input batches, one warmup,
three measured trials, asynchronous acknowledgement and a release build. The test
JVM has a 2 GiB heap; each Fluss broker has a 1 GiB heap and a 512 MiB direct-memory
limit. These are local Linux measurements, not the README's Apple M1 Max Kafka
measurements. All trials are retained below, including the stock wide NONE read outlier.
The measured boundaries and fixtures are described above. Milliseconds:

| Fixture / codec | Arrow append | Stock append | Arrow read | Stock read |
| --- | --- | --- | --- | --- |
| Narrow / NONE | 66.327 / 47.177 / 44.180 | 135.042 / 164.185 / 106.030 | 60.739 / 38.077 / 44.049 | 113.405 / 95.694 / 173.970 |
| Narrow / LZ4_FRAME | 38.935 / 38.812 / 39.225 | 117.236 / 116.310 / 130.303 | 42.266 / 43.669 / 41.097 | 98.108 / 90.364 / 91.421 |
| Narrow / ZSTD | 53.376 / 49.086 / 55.228 | 94.476 / 91.916 / 111.386 | 27.223 / 27.129 / 26.927 | 75.410 / 76.352 / 63.827 |
| Wide / NONE | 244.941 / 251.571 / 217.394 | 257.472 / 260.633 / 285.012 | 237.527 / 174.942 / 186.097 | 1025.163 / 379.704 / 441.669 |
| Wide / LZ4_FRAME | 273.601 / 262.686 / 276.463 | 420.114 / 439.836 / 418.922 | 200.588 / 207.727 / 189.878 | 322.537 / 324.273 / 321.983 |
| Wide / ZSTD | 226.819 / 231.149 / 224.852 | 335.954 / 354.661 / 327.245 | 249.606 / 248.428 / 237.400 | 374.748 / 342.893 / 347.975 |

Narrow append medians improve on the stock Java writer by 2.86× / 3.01× / 1.77×;
read medians improve by 2.57× / 2.16× / 2.78× for NONE / LZ4_FRAME / ZSTD.
For the wide fixture, append gains are 1.06× / 1.54× / 1.48×. The wide NONE append
advantage is small, and its stock read timings vary substantially. These measurements
establish transport behavior, not a full-job or absolute Kafka comparison.

The controlled pipeline comparison disables only request coalescing; encoding,
batching and acknowledgement limits remain the same:

| Narrow codec | Previous pipeline append, ms | Small-request coalescing append, ms | Median improvement |
| --- | --- | --- | ---: |
| NONE | 127.698 / 85.911 / 77.724 | 66.327 / 47.177 / 44.180 | 1.82× |
| LZ4_FRAME | 68.136 / 61.420 / 81.813 | 38.935 / 38.812 / 39.225 | 1.75× |
| ZSTD | 54.109 / 70.249 / 56.207 | 53.376 / 49.086 / 55.228 | 1.05× |

Uncapped grouping of the wide NONE fixture produced 299.487 / 273.179 / 276.437 ms,
versus 266.728 / 197.292 / 215.678 ms with grouping disabled. It reduced RPC count
from 128 to 27 but increased elapsed time. The production 1 MiB cap keeps these wide
batches in individual requests; the capped trials appear above. Narrow batches remain
below the cap, so their grouping behavior is unchanged.

The warm JNI identity projection shares 131,072 data-buffer bytes for the narrow batch,
with 2,048 validity-buffer bytes changing address. For the wide batch, NONE shares
1,131,140 bytes and changes 133,120 bytes; LZ4_FRAME and ZSTD share 1,262,212 bytes
and change 2,048 bytes. The larger NONE change includes decimal alignment. All returned
values are checked; address changes are not reported as a general copied-byte count.

## Production timestamp layout measurements

The final wide fixture uses the production component timestamp layout for both read
and append. Configuration remains 1,048,576 rows, batches of 8,192, one bucket,
one warmup, three release-mode trials and asynchronous acknowledgements. All timings
are milliseconds; input generation and JNI identity validation remain outside timing.
Timestamp layout conversion is inside the timed transport path.

| Compression | Arrow append | Stock append | Arrow read | Stock row read |
| --- | --- | --- | --- | --- |
| NONE | 216.040 / 194.969 / 243.136 | 252.576 / 275.966 / 227.067 | 241.374 / 163.302 / 189.965 | 385.416 / 348.421 / 336.603 |
| LZ4_FRAME | 268.242 / 258.254 / 247.766 | 408.204 / 402.672 / 401.493 | 204.920 / 193.621 / 179.113 | 337.219 / 333.334 / 308.096 |
| ZSTD | 225.885 / 226.571 / 222.308 | 315.006 / 326.738 / 326.360 | 250.082 / 244.093 / 253.717 | 346.826 / 354.268 / 365.917 |

Median append speedups over stock are 1.17× / 1.56× / 1.44×; read speedups are
1.83× / 1.72× / 1.42×, respectively. A separate controlled run disabled borrowing
millisecond input buffers, retaining the previous rowwise timestamp conversion:

| Compression | Rowwise-layout Arrow read | Rowwise decode/ownership | Borrowed-layout decode/ownership |
| --- | --- | --- | --- |
| NONE | 293.177 / 172.685 / 192.602 | 70.541 / 28.358 / 30.714 | 41.460 / 21.851 / 25.784 |
| LZ4_FRAME | 200.635 / 192.904 / 199.970 | 68.375 / 66.488 / 69.260 | 66.572 / 61.651 / 58.509 |
| ZSTD | 239.674 / 231.196 / 255.752 | 106.584 / 103.271 / 106.667 | 100.595 / 96.964 / 100.563 |

Borrowing improves median decode/ownership by 1.19× / 1.11× / 1.06×. Whole-read
medians improve by 1% and 3% for NONE and LZ4, while ZSTD is 4% slower in this pair
of runs despite lower decode cost; RPC variability remains material. The optimization
removes the millis copy, but these trials do not establish a whole-read ZSTD win.
The recursive JNI handoff check observes 1,163,908 shared buffer bytes and 135,168
changed-address bytes for NONE, and 1,294,980 shared / 4,096 changed for LZ4 and ZSTD.
All returned values, including timestamp components and nulls, are validated. These
are address observations, not inferred copied-byte counts.
