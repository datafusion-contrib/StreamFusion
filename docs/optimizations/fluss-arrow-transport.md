# Fluss Arrow transport

The optional Fluss connector preserves the broker's columnar batches across the Java
boundary, then uses the existing Arrow C Data Interface bridge to Rust. It uses the
released Java client's connection and RPC implementations; it does not fork Fluss or
incorporate fluss-rust.

## Receive ownership

An addressable direct RPC buffer is retained as an Arrow foreign allocation. The
allocator charges the complete allocation, while the IPC record batch takes slices of
its body. Closing the RPC response releases its original reference; closing the final
Arrow vector releases the retained allocation. Tests check addresses, reference counts,
allocator accounting and rejected reservations.

Uncompressed compatible vectors share the received body. Compression needs new
decompression buffers, timestamp representation conversion can allocate, and Rust's
alignment checks can require copying. Heap responses and remote-file reads use the
copying decoder. These boundaries prevent a claim that the full path is zero copy.

An actual broker batch with 2,048 mixed-type rows was exported through the production
identity Calc and imported back into Java, with every value checked. Uncompressed
input retained 282,756 buffer bytes at the same addresses; 33,280 buffer bytes changed
addresses. LZ4 and ZSTD retained 315,524 bytes and changed 512 bytes after their required
decompression. These are buffer-address observations from a correctness smoke run,
not copied-byte counts or throughput results. They demonstrate why sharing must be
verified through Rust rather than inferred from Java's received-buffer slices alone.

## Append serialization and acknowledgements

Serialization writes into one exactly sized byte array, removing stream growth and the
final `toByteArray` copy. The released client request encoder then copies that array into
the outbound RPC buffer. The server's memory-segment send optimization does not remove
this client-side copy.

Encoding runs on the task thread and acknowledgements run asynchronously. A bounded
queue overlaps encoding with RPC work and permits independent buckets to progress
concurrently. Each bucket keeps a sequential future chain, including retries, so its
writer sequence cannot be reordered. Checkpoint flush waits for every acknowledgement.
Already queued batches can share one request without concatenating their wire arrays:
the released client's composite byte view traverses the separate arrays during its
normal outbound copy. Groups are limited to 1 MiB and five batches, matching the broker's retained
sequence history, and to the writer request limit. Input roots larger than the normal
writer batch target are sliced columnarly before encoding; exceptional encoded sizes
are split further until they fit, or fail if a single record is too large.
Immutable request arrays let input Arrow roots close immediately; queue reservations
remain charged to the shared task memory budget until acknowledgement or cancellation.

Millisecond timestamp input retains the wire millis and parent-validity buffers;
a zero fractional-nanos buffer and small all-valid child bitmap provide StreamFusion's
component layout without per-row timestamp objects or millis copies. For millisecond
output, the component representation's millis and validity buffers are shared with the
wire timestamp vector. Other timestamp conversions retain
their required conversion path.

## Measurements

The [connector's transport fixture](../connectors/fluss.md#isolated-transport-profiling)
retains serialization, compression, routing, RPC and acknowledgements. It excludes
Flink startup, JNI and query execution. On the initial two-BIGINT, one-bucket fixture
(1,048,576 rows, batches of 8,192, warmup plus three release-mode trials), exactly sized
serialization reduced median sequential ZSTD append time from 131.151 to 116.550 ms.
Acknowledgement pipelining measured 66.704 / 57.219 / 62.394 ms, a median 1.87× improvement
over sequential acknowledgements. Encode and RPC stage durations overlap in this path.
The bounded pipeline comparison now measures narrow NONE append medians of 85.911 ms
without request grouping and 47.177 ms with small-request grouping (1.82×). LZ4_FRAME
improves from 68.136 to 38.935 ms (1.75×); ZSTD improves from 56.207 to 53.376 ms
(1.05×), with encoding dominating that path. Grouping wide NONE batches into large
requests regressed from 215.678 to 276.437 ms despite reducing RPC count; production
therefore caps groups at 1 MiB. Wide batches retain individual requests. All trial
values and the matched stock Java writer results are recorded on the connector page.

The final wide fixture includes production timestamp component conversion. Borrowing
the received millisecond data reduces median decode/ownership from 30.714 to 25.784 ms
(NONE), 68.375 to 61.651 ms (LZ4), and 106.584 to 100.563 ms (ZSTD). Whole-read ZSTD
is 4% slower between the control runs despite lower decode cost, so that result is
retained rather than described as a whole-read win. With borrowing enabled, matched
stock/Arrow median speedups are 1.17× / 1.56× / 1.44× for append and 1.83× / 1.72× /
1.42× for read. The connector page retains every read, append and decode trial.

These measurements establish a transport improvement for those fixtures. They do not
establish a full Nexmark speedup or a Kafka comparison. The full suite retains query,
JNI, timestamp conversion, checkpoint and stock primary-key sink costs.
