# Apache Fluss

**Status:** Experimental, opt-in. The optional `streamfusion-fluss` module uses the
released Apache Fluss Java connector 1.0.0. Enable its verified planner substitutions
with `-Dstreamfusion.fluss.enabled=true`. All 23 runnable Nexmark queries pass, with
matching deterministic output. The narrower verified connector boundary and
short-job-dominated benchmark evidence keep the stock connector as the default.
[Final 2M-event results](#final-nexmark-validation) include all append-only trials and
separate primary-key correctness pairs.

The integration uses the existing Java connection for metadata, security, routing and
broker RPCs. Concurrent Arrow readers and writers with identical complete client
configuration share a reference-counted connection within the module's classloader.
Each reader keeps its own split/checkpoint state and each writer keeps its own ID,
sequences and acknowledgement queue. Closing one owner leaves the other owners usable;
the last owner removes and closes the connection synchronously, with no idle cache or
background teardown. Configuration is copied before the Java client can mutate it.

It reads schema-less Arrow IPC messages from `FetchLog`, preserving the
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

The accelerated connection selects direct receive allocations instead of the released
SDK's default heap-preferring accumulator. Its length-aware accumulator reserves a
complete direct frame once the four-byte length is available, avoiding repeated growth
copies for fragmented large replies. It preserves the released channel initializer,
handshake, security and idle handlers. Installing it accesses the pinned SDK's private
connection/bootstrap fields; unavailable fields or reflective access cause planner
fallback before acceleration. This is an explicit version-specific transport hook,
not a new public Fluss API.

Direct RPC receive allocations are retained through Arrow's foreign-allocation API and
charged to its allocator until the last vector reference closes. IPC metadata is parsed
separately, and the body buffers reference the received allocation. Uncompressed bodies
avoid a network-buffer-to-Arrow body copy; compressed bodies still need decompression
allocations. Non-direct buffers and remote files use the copying decoder. Rust may copy
under-aligned buffers during import, and high-precision timestamp conversion also allocates;
the complete path is not claimed to be zero copy. Append serialization uses one exactly
sized wire byte array, avoiding growing streams and their final array copy; the released Java request encoder still copies it into the outbound RPC buffer.
Fluss's memory-segment zero-copy send path is used for server responses, not client requests.
LZ4 uses the released Fluss block streams with a bounded 8 KiB adapter scratch buffer
and Arrow-backed output, eliminating whole-vector heap staging and growing output
streams. Compression temporarily reserves an Arrow output sized for the uncompressed
input plus frame overhead, then serializes only its written extent. This reservation
is allocator-accounted even for highly compressible input. The released compressor
still allocates its block workspace; decompression
still creates new Arrow buffers. NONE and ZSTD retain their released codecs.

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
other combinations retain the stock connector. The Fluss-to-Fluss Nexmark harness
reuses the Kafka suite's queries and corpus. Its completed results and isolated
transport measurements are below; they establish improvements within the measured
boundaries, rather than uniform speedups for every query or deployment.

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

The opt-in matrix harness runs with the release profile. It accepts the same query selector
as the Kafka matrix and reports all individual repetitions rather than a single best run. Each native plan
must contain native interior operators, with a row boundary permitted only for the
intentionally stock primary-key sink. Its not-null constraint and stream-record
timestamp insertion operators are permitted only downstream of that transpose:

```sh
SF_FLUSS_BENCH=true SF_MATRIX_QUERIES=q0 SF_ROWS=1000000 mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=NexmarkFlussBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dnexmark.warmups=1 -Dnexmark.runs=3
```

Input seeding, output validation and table cleanup occur outside the measured job duration.
Both runs use the same preloaded four-bucket input, queries, watermarks, parallelism and
checkpoint interval. Primary-key output remains the stock sink in the native run. The
harness compares exact output multisets using bounded-memory external sorting outside
the timed execution, including duplicate counts; q12's processing-time windows cannot use wall-clock output equality as a parity assertion.
The static lookup result in q13 is deterministic and remains subject to output parity.

Kafka references use the existing headline results in `readme.md`; this integration
does not rerun Kafka benchmarks. Comparisons cover only append-only output queries.
The published Kafka table reports StreamFusion/Flink speedups on an Apple M1 Max;
this machine's Fluss measurements have their own configuration and cannot establish
an absolute cross-machine Fluss/Kafka throughput ratio. The README's expression variants
and exactly-once Kafka delivery also differ from the exact Fluss fixture's settings.

The input event table is append-only. Output tables are primary-key tables only for
q4, q9, q15, q16, q17, q18 and q19, following the Kafka suite's existing upsert keys.
The remaining queries write append-only tables and must engage the Arrow append sink.
The harness checks the actual broker table modes as well as the executed source/sink plans.

## SQL regression tests and CI

`FlussSqlTest` adapts the SQL scenarios from the released Apache Fluss
[v1.0.0 source integration tests](https://github.com/apache/fluss/blob/v1.0.0/fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/source/FlinkTableSourceITCase.java)
and [sink integration tests](https://github.com/apache/fluss/blob/v1.0.0/fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/FlinkTableSinkITCase.java)
to the released Docker broker fixture. It does not import the upstream in-process
mini-cluster or claim to port every upstream partition, security, tiering or failover
case. The supported/fallback scenarios are:

- Append projection/reordering for ARROW and INDEXED (INDEXED stays stock).
- Primary-key projected log changes with FULL/WAL images, updates and deletes.
- Append production with NONE/LZ4_FRAME/ZSTD and the upstream example rows; bounded
  Fluss input exercises the native Arrow source and sink instead of a literal source.
- The upstream literal `INSERT ... VALUES` append/primary-key cases; unsupported
  literal sources and primary-key writes retain stock production.
- Added nullable columns with historical rows: full-column stock/native SQL parity,
  native projected null filling, and an explicit expected stock 1.0 projection failure
  (`INVALID_COLUMN_PROJECTION`) when a new column is absent from an older batch.

All 10 adapted cases pass locally on Flink 2.2 and 1.18. Tests compare output with
stock Flink, assert expected values/changelog kinds, and save
actual physical plans while checking acceleration or fallback. The ordinary CI Java
reactor runs these tests on Flink 2.2 and 1.18. The optimized Flink 2.2 image job also
explicitly enables the full 23-query Nexmark parity smoke at 8,192 events, with no
performance assertions. It reuses that job's staged release library, runs zero warmups
and one pair per query, and uploads SQL/Nexmark plans and test reports. The environment
opt-in still protects normal local builds from unexpectedly running the full matrix.

```sh
mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=FlussSqlTest -Dsurefire.failIfNoSpecifiedTests=false
```

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
faster serialization of the same number of requests. `receivedRecordBytes` counts
received record spans, including log headers, compressed payloads and possible partial
batch tails; `borrowedRecordBytes` counts those spans backed by retained direct
allocations. They diagnose the heap/direct receive boundary, not copied-byte volume
or Java-to-Rust sharing.
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

These are historical measurements before direct receive was configured, as well as
before receive-buffer retention and exactly sized append
serialization. JFR heap allocation samples identified growing byte streams and their final
array copies as producer costs; sampled allocation weights are not copied-byte counts.

After receive-buffer retention and exactly sized serialization, still using heap
receive before the current direct receiver, the same fixture measured the following milliseconds. Profiling counters were enabled;
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

The first complete 8,192-event sweep passed every runnable query (q0–q23 except q6),
including deterministic output parity. With pipelining enabled, the isolated ZSTD append
fixture measured 66.704 / 57.219 / 62.394 ms, versus 119.923 / 116.550 / 107.761 ms for
sequential acknowledgements. Encoding and RPC durations overlap in the pipelined path.
The completed two-million-event suite is recorded below; these early smoke results
are not used as throughput headlines.
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

## Completed Nexmark validation

All 23 runnable queries pass at 2,000,000 events with native source and query-interior
checks. Append-only outputs also require the Arrow append sink. Primary-key outputs
retain their stock sink and validate its transpose/constraint boundary. Exact output
multisets, including duplicate counts, match for every deterministic query. q12's
processing-time output is exempt; q13's static lookup result remains checked. q6 is
excluded by the shared Kafka fixture because Flink SQL cannot run it.

The full-matrix runs below precede connection sharing and direct receive/codec
optimizations. Focused current results follow them. These historical runs use released Fluss 1.0.0, four buckets, parallelism four, UTC, a one-second
checkpoint interval, default memory state and mini-batching off. The test JVM uses a
2 GiB heap; each broker uses a 1 GiB heap and a 512 MiB direct-memory limit. Each query
starts in a fresh test JVM and broker cluster. Append-only queries have one stock/native
warmup pair and three measured pairs. Primary-key queries have one final correctness
pair, following their earlier repeated validation; these are not performance estimates
or Kafka comparisons. Setup, input seeding, output sorting/parity and cleanup are outside
timing. Each duration includes executeSql planning/startup, execution and client teardown.

### Append-only output trials

| Query | Stock Fluss seconds | StreamFusion Fluss seconds | Median SF/Flink |
| --- | --- | --- | ---: |
| q0 | 4.896173 / 4.983536 / 9.820781 | 4.728846 / 4.557351 / 4.506870 | 1.09× |
| q1 | 4.979650 / 4.908936 / 4.850540 | 4.584251 / 9.651440 / 4.518153 | 1.07× |
| q2 | 4.790409 / 4.628008 / 4.587244 | 4.456956 / 4.628614 / 4.356201 | 1.04× |
| q3 | 7.608857 / 4.618437 / 7.526865 | 4.661656 / 4.662505 / 4.582065 | 1.61× |
| q5 | 5.653263 / 5.329129 / 5.092166 | 5.377029 / 5.324281 / 5.123020 | 1.00× |
| q7 | 12.943312 / 14.161689 / 5.806671 | 21.144914 / 5.959881 / 6.000944 | 2.16× |
| q8 | 5.029525 / 7.691730 / 4.689957 | 5.095309 / 4.712600 / 4.812821 | 1.05× |
| q10 | 5.400998 / 5.156140 / 5.451437 | 10.185726 / 4.914559 / 4.924163 | 1.10× |
| q11 | 5.244802 / 5.057556 / 5.004168 | 4.836226 / 4.679192 / 4.644174 | 1.08× |
| q12 | 5.168656 / 4.763380 / 4.729435 | 4.498994 / 4.436776 / 4.397046 | 1.07× |
| q13 | 4.978735 / 4.903931 / 4.862256 | 4.704918 / 4.595521 / 4.582493 | 1.07× |
| q14 | 5.195299 / 5.138232 / 5.144532 | 5.239781 / 10.053488 / 5.137880 | 0.98× |
| q20 | 8.263257 / 8.294414 / 5.316719 | 5.948705 / 8.441064 / 5.663681 | 1.39× |
| q21 | 5.083205 / 4.946829 / 5.146156 | 5.028118 / 4.938488 / 4.926485 | 1.03× |
| q22 | 5.149269 / 10.012365 / 5.282074 | 4.591372 / 4.573737 / 4.575941 | 1.15× |
| q23 | 11.314178 / 8.466644 / 11.235197 | 5.621627 / 5.599494 / 5.559362 | 2.01× |

The historical append-only geomean of median speedups is 1.20×. Its q14 regresses by about 2%;
several queries have outliers, including q7's native 21.145-second trial. No trials
are discarded. These results demonstrate a working columnar pipeline, with modest
startup-dominated gains on many queries; they do not establish that Fluss is uniformly
faster than the README's Kafka measurements. The Kafka values in the landing page are
existing published references on another machine, with the documented expression and
delivery differences. The integration remains off by default. The focused reruns below resolve the measured q14 regression. [Follow-up #301](https://github.com/datafusion-contrib/StreamFusion/issues/301) tracks the remaining lifecycle floor and broader sustained validation.

### Primary-key output correctness runs

These final single pairs pass exact output parity and native query-interior assertions.
They use the stock primary-key writer and are recorded without a Kafka comparison:

| Query | Stock Fluss seconds | StreamFusion query / stock writer seconds |
| --- | ---: | ---: |
| q4 | 10.415635 | 7.399418 |
| q9 | 10.449697 | 11.439946 |
| q15 | 9.082497 | 7.831368 |
| q16 | 10.899953 | 9.680526 |
| q17 | 10.850807 | 9.934540 |
| q18 | 9.373285 | 8.692731 |
| q19 | 35.793688 | 16.450879 |

## Matched hot-path profiling and connection reuse

JFR profiles of stock and native q0/q14 at 2M events separate source-task/fetcher
threads from fixture setup and output validation. Stock execution samples prominently
include boxed integers, string materialization and row copies. Native q14 prominently
includes generated Java expression evaluation and its Arrow row views; native plan
coverage does not imply that every expression is evaluated in Rust.

Each native source/sink task recorded two approximately 2.005-second waits inside
`NettyClient.close`, while stock source fetching closes on its separate fetcher thread.
The native chain was serially shutting down independent source and sink transports.
Reference-counted connection reuse removes redundant clients and shutdowns without
subtracting cleanup or moving it outside the job's measured lifetime.

The control disables only `streamfusion.fluss.connection-sharing.enabled`. Released
Fluss 1.0.0, 2M events, four buckets/parallelism four, memory state, mini-batching off,
one warmup and three measured pairs remain identical. All deterministic output and
native-plan assertions pass. Every trial is retained, including q20's large outliers.
Seconds:

| Query / transport lifetime | Stock Flink | StreamFusion |
| --- | --- | --- |
| q0 / independent connections | 5.016760 / 4.978390 / 9.903302 | 6.444252 / 4.530517 / 4.540318 |
| q0 / shared connection | 5.207928 / 4.922891 / 4.954520 | 2.487257 / 2.445463 / 2.409509 |
| q14 / independent connections | 5.085205 / 5.138027 / 5.027056 | 4.940219 / 4.976899 / 4.954791 |
| q14 / shared connection | 5.012289 / 5.096018 / 5.051965 | 2.909993 / 2.919888 / 2.898332 |
| q20 / independent connections | 5.465149 / 29.356056 / 14.207900 | 12.521662 / 30.652372 / 5.827588 |
| q20 / shared connection | 5.546512 / 8.048414 / 8.065166 | 3.705364 / 3.440408 / 3.398679 |

Native medians improve over independent connections by 1.86× / 1.70× / 3.64× for
q0 / q14 / q20. Against matched stock Flink, shared-connection speedups are 2.03× /
1.74× / 2.34×. q20's comparison has substantial variability; its improvement must not
be attributed entirely to lifecycle. These focused trials supersede the earlier
lifecycle behavior, but are not a new full-suite geomean.

To capture matched profiles, add a JVM recording to the same release harness:

```sh
SF_FLUSS_BENCH=true SF_MATRIX_QUERIES=q0,q14 SF_ROWS=2000000 mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=NexmarkFlussBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dnexmark.warmups=1 -Dnexmark.runs=1 \
  '-Dsf.extraJvmArgs=-XX:StartFlightRecording=filename=fluss-hotpaths.jfr,settings=profile,dumponexit=true'
```

Inspect execution samples, allocation samples and thread parks. Attribute samples by
source/fetcher thread and stack; seeding, stock output scanning and sorting are outside
timing and must not be mistaken for native query costs. JFR trials diagnose costs;
headline timings use repeated runs without recording overhead.

## Direct receive and bounded LZ4 controls

These transport controls use the same released broker, one bucket, 1,048,576 rows in
8,192-row batches, a 2 GiB test JVM, one warmup and three measured trials. Append uses
the production acknowledgement pipeline. Wide inputs include the string, decimal and
production timestamp component layout described above. Read timing retains its checksum;
JNI address checks run outside timing. No Flink lifecycle or query costs are included.
[All 90 trials and stage durations](../benchmarks/fluss-transport-controls.csv) retain
stock comparisons and unfavorable measurements. `run=0` is the first measured trial;
the warmup is separate. Diagnostic switches, applied inside the opt-in connector, are:

- `streamfusion.fluss.direct-receive.enabled=false`: released heap-preferring receive.
- `streamfusion.fluss.frame-aware-receive.enabled=false`: generic direct accumulation.
- `streamfusion.fluss.lz4-streaming.enabled=false`: released whole-vector LZ4 adapter.

All three default to true. The receive/codec controls were captured before the
frame-aware receiver was installed; their `directTrue` mode is generic direct receive.
The frame-aware controls hold direct receive and streaming LZ4 enabled, changing only
the accumulator. Median milliseconds from the final frame-aware control:

| Input / codec | Generic direct read | Frame-aware direct read | Stock row read | Arrow append | Stock row append |
| --- | ---: | ---: | ---: | ---: | ---: |
| narrow / NONE | 47.909 | 51.501 | 187.436 | 44.111 | 111.880 |
| narrow / LZ4_FRAME | 40.813 | 35.034 | 88.774 | 32.184 | 108.836 |
| narrow / ZSTD | 26.790 | 25.728 | 64.686 | 56.676 | 100.110 |
| wide / NONE | 203.473 | 173.883 | 350.841 | 212.937 | 236.606 |
| wide / LZ4_FRAME | 218.841 | 189.095 | 319.633 | 183.737 | 401.091 |
| wide / ZSTD | 273.821 | 229.987 | 334.174 | 220.574 | 338.870 |

Wide read medians improve by 1.17× / 1.16× / 1.19× over generic direct accumulation.
Narrow NONE whole-read median regresses 8%, although its median fetch stage falls
from 39.405 to 34.628 ms; this regression is retained. Narrow LZ4/ZSTD improve by
1.16×/1.04×. Against the matched stock Java reader, current wide read speedups are
2.02×/1.69×/1.45×. Wide append speedups are 1.11×/2.18×/1.54×. These are isolated
transport results, not Nexmark or cross-machine Kafka throughput claims.

With direct receive held fixed in the earlier codec controls, streaming LZ4 reduces
median narrow append from 38.835 to 32.530 ms and wide append from 264.357 to
178.357 ms (1.19×/1.48×); wide decode falls from 60.215 to 50.566 ms. Narrow whole
LZ4 read regresses from 43.338 to 46.030 ms in those controls. Turning on generic
direct receive alone also regresses some wide reads; the length-aware receiver above
addresses the fragmentation cost rather than assuming direct memory is inherently faster.

The broker integration test proves `borrowedRecordBytes == receivedRecordBytes > 0`
for direct receive, versus zero borrowed bytes for heap receive, with identical rows
and projection. This does not prove end-to-end zero-copy import. In the current real
broker JNI identity check, narrow NONE changes 133,120 buffer bytes and shares none:
its numeric buffers are under-aligned at their RPC offsets. Narrow LZ4/ZSTD share
131,072 bytes and change 2,048 after required decompression. Wide NONE shares 934,528
bytes and changes 364,548; wide LZ4/ZSTD share 1,294,980 and change 4,096. These are
recursive address observations, not copied-byte counters. Earlier heap-receive sharing
observations were downstream of an already-copied Java Arrow body.

### Remaining hot-path costs and rejected work

Released Fluss and Kafka clients group requests by broker and pipeline bounded work.
The four-bucket/four-reader Nexmark topology assigns one bucket per reader, so a
multi-bucket fetch grouping rewrite would not improve this workload. A bounded
one-request source prefetch prototype produced only about 1% q0 and 0.3% q14 median
improvement at 2M events, within run variability. It was removed, including its extra
queue/checkpoint state; [the rejection and every trial](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/fluss-source-prefetch.md)
are retained.

Remaining substantial costs are Java expression/decimal/string evaluation in q14,
required native alignment work, compression/decompression, the released request
encoder's copy into its direct outbound buffer, and the final connection's roughly
two-second Netty quiet shutdown. Cleanup stays inside end-to-end timing. Removing
these costs requires broader expression work, a compatible alignment/wire solution,
or an upstream transport/lifecycle API; no speculative fork or idle-client cache is
introduced by this optimization pass.

## Focused profile-validation reruns

After connection reuse, direct frame-aware receive and bounded LZ4, a fresh release
run selects q0/q14/q20 at 2M events, with one warmup and three measured stock/native
pairs per query. The selection shares one test JVM, broker cluster and seeded corpus;
per-query warmups remain enabled. Resources, four buckets/parallelism four, memory
state, mini-batching off and exact SQL are unchanged. No JFR recording is enabled;
planning/startup, execution and client teardown remain timed. Every exact output and
native source/interior/append-sink assertion passes. Seconds:

| Query | Stock Flink trials | StreamFusion trials | Median SF/Flink |
| --- | --- | --- | ---: |
| q0 | 4.982779 / 4.798983 / 4.764074 | 2.516900 / 2.482839 / 2.436736 | 1.93× |
| q14 | 5.026582 / 5.039969 / 5.022448 | 2.916753 / 2.911917 / 2.914523 | 1.72× |
| q20 | 5.377090 / 8.183366 / 5.317126 | 3.555080 / 3.480970 / 3.606993 | 1.51× |

These profile-driven trials preceded the complete final matrix below; they do not
define a full-suite geomean. q20 retains a stock 8.183-second outlier. The complete
23-query matrix passed again at 8,192 events after these optimizations, including
stock primary-key sinks; this correctness sweep is not a throughput measurement.
The connector/schema/ownership/admission/output contract sweep passed 41 tests.

A fresh matched q0/q14 JFR recording at 2M events includes one warmup and one measured
pair. It records one approximately 2.01-second native task close wait per job, down
from two waits in each of four native task chains before sharing. Stock task chains
still each record one close wait, with source-fetcher cleanup on its own thread. This
verifies the lifetime change without subtracting cleanup. The earlier recording had
no warmup, so raw sample totals cannot serve as normalized before/after CPU speedups.

In the current recording's captured source-task stacks, stock top samples include
string materialization (45), row copying (38), boxed longs (35) and accumulator append
(31). Native stacks prominently include generated Java expression evaluation (31),
JNI invocation (24), timestamp extraction (24), row copying (19) and boxing (14).
These are sampling observations, not exact per-stage times; JFR does not resolve Rust
execution below the JNI frame. They support the remaining expression/representation
work above, rather than attributing every native sample to network overhead.

## Final Nexmark validation

The complete final matrix uses released Fluss 1.0.0, Flink 2.2, 2,000,000 events,
four buckets, parallelism four, UTC, one-second checkpoints, memory state and
mini-batching off. The test JVM has a 2 GiB heap, each broker has a 1 GiB heap and
512 MiB direct-memory limit, and the native library is the release/mimalloc build.
All 16 append-only queries share one freshly started test JVM, cluster and seeded
corpus, with a stock/native warmup pair and three measured pairs for each query.
The seven primary-key output queries run in a separate fresh JVM and cluster with
one correctness pair each. These differ from the historical per-query-JVM runs;
they are not an identical before/after control. The focused lifecycle/codec controls
above isolate the optimizations independently.

Planning/startup, execution and client teardown stay inside job timing. Cluster
setup, input seeding, output scanning/sorting/parity and cluster cleanup remain outside.
No recording overhead is included. All deterministic output multisets, including
duplicates, match stock Flink; q12 retains its processing-time exemption. Every
native source/interior assertion passes. All append-only outputs execute the Arrow
sink, and primary-key production remains stock. q6 remains excluded by the shared
fixture because Flink SQL cannot execute it. [All 55 final measured pairs](../benchmarks/fluss-nexmark-final.csv)
are retained; no trial is discarded. Complete job seconds:

| Append-only query | Stock Flink trials | StreamFusion trials | Median SF/Flink |
| --- | --- | --- | ---: |
| q0 | 4.862733 / 4.794690 / 4.736671 | 2.518370 / 2.469245 / 2.440789 | 1.94× |
| q1 | 4.818989 / 4.771118 / 4.742557 | 2.426037 / 2.413878 / 2.456827 | 1.97× |
| q2 | 9.456386 / 4.540128 / 9.595717 | 2.311209 / 2.289842 / 2.302503 | 4.11× |
| q3 | 4.522304 / 4.523334 / 4.492609 | 2.523294 / 2.533506 / 2.514319 | 1.79× |
| q5 | 5.118286 / 4.942575 / 4.940154 | 3.080059 / 3.087738 / 3.081525 | 1.60× |
| q7 | 5.803485 / 5.623989 / 5.574316 | 4.017014 / 3.974940 / 3.893690 | 1.41× |
| q8 | 4.542199 / 4.550724 / 4.561198 | 2.521938 / 2.521738 / 2.544414 | 1.80× |
| q10 | 5.083663 / 5.067742 / 4.967778 | 2.826109 / 2.820590 / 2.785411 | 1.80× |
| q11 | 5.015348 / 4.932876 / 4.937438 | 2.340103 / 2.344385 / 2.334384 | 2.11× |
| q12 | 4.612266 / 4.599478 / 4.647023 | 2.312819 / 2.302554 / 2.305954 | 2.00× |
| q13 | 4.782113 / 4.824035 / 4.925995 | 2.419755 / 2.400287 / 2.381811 | 2.01× |
| q14 | 5.006960 / 5.017084 / 4.995941 | 2.905087 / 2.883766 / 2.861918 | 1.74× |
| q20 | 31.841183 / 5.312361 / 7.847848 | 8.953495 / 3.498312 / 3.467522 | 2.24× |
| q21 | 4.904971 / 4.806890 / 4.761857 | 2.838190 / 2.758646 / 2.764158 | 1.74× |
| q22 | 4.970074 / 4.951053 / 5.006223 | 2.383224 / 2.410302 / 2.369573 | 2.09× |
| q23 | 28.147132 / 10.862695 / 8.496231 | 3.843080 / 3.525013 / 3.481148 | 3.08× |

The geomean of append-only median speedups is **2.02×**, with all 16 medians
faster than matched stock Flink. q14 is 1.74×, resolving the previous regression.
q2, q20 and q23 have large stock outliers; q20 also has an 8.953-second native trial.
Their elevated ratios must not be interpreted as clean steady-state processing gains.
The jobs remain short and lifecycle-heavy: the measured removal of a redundant
approximately two-second shutdown is the largest end-to-end improvement. The isolated
transport controls demonstrate the additional receive/compression improvements.
No Kafka benchmarks were rerun; published README references are comparisons of
relative speedup on another machine, with different expression/delivery settings.

Primary-key output timings below are single correctness observations, not performance
estimates or Kafka comparisons. The source and query interior are native; the writer
remains stock. Exact output parity passes for every pair:

| Primary-key output query | Stock Flink seconds | StreamFusion interior / stock writer seconds |
| --- | ---: | ---: |
| q4 | 10.224197 | 7.140303 |
| q9 | 8.435984 | 7.567584 |
| q15 | 10.294327 | 7.452906 |
| q16 | 6.724553 | 6.778204 |
| q17 | 9.055959 | 6.310909 |
| q18 | 10.539551 | 8.246340 |
| q19 | 39.652596 | 15.865008 |

### CI packaging and offset alignment

The optional Fluss artifact resolves its published coordinates with the same `ossrh`
POM flattening as the other connectors. Flink 1.18's overridden Javadoc dependency
list includes the released Fluss connector so the shared source path remains resolvable;
this does not add Fluss to the runtime or core consumer dependencies.

Borrowed uncompressed string/binary offset buffers may start at an unaligned RPC address.
The decoder copies only an offset buffer whose address is not aligned to four bytes,
before exporting it to native code. Arrow-rs 58 reads the final variable-width offset
inside FFI import, before the bridge's existing `align_buffers` repair can run.
Validity and variable-width data buffers remain borrowed. This check is required in
release builds as well as debug builds; release execution succeeding did not prove
that an unaligned dereference was safe. A regression exercises all four address residues,
null values, and retained data-buffer sharing.

### Selecting disk state for the Fluss matrix

`SF_FLUSS_STATE_BACKEND=rocksdb` selects Flink's released RocksDB backend for stock
jobs and `RocksDBNativeStateBackendFactory` for native jobs, matching the existing
persistent-state comparison. Both engines receive a fixed 128 MiB RocksDB memory
pool per slot. Native RocksDB and any JVM fallback delegate retain their separate
resource pools, as in the production backend; this is not a combined process-memory
cap. This avoids the local mini-cluster default allocating roughly 3.3 MiB of
write-buffer memory for the q4 preflight, which produced repeated tiny flushes and
write stalls. That initial untimed attempt was stopped before any measured pair. The default remains `memory`; mini-batching is explicitly
disabled for both engines. Before measured disk trials, an untimed q4 preflight verifies
stock RocksDB working files and a live native RocksDB handle. Stateless queries still
use the selected job configuration without manufacturing state. A temporary local
state directory is cleaned after the cluster closes.

```sh
SF_FLUSS_BENCH=true SF_FLUSS_STATE_BACKEND=rocksdb SF_ROWS=2000000 \
mvn -Pbench -pl streamfusion-fluss -am test -Dtest=NexmarkFlussBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.extraJvmArgs=-Xmx2g \
  -Dnexmark.warmups=1 -Dnexmark.runs=3
```

Use `SF_MATRIX_QUERIES` to select the same append-only or primary-key output groups
listed above. Queries, schemas, event corpus, watermarks, source/sink semantics and
parity checks are unchanged by the backend selector. Disk state is an engine setting;
it does not change the Fluss broker's storage configuration. CI also exercises q4
and q5 with disk state in the optimized Flink 2.2 job.
