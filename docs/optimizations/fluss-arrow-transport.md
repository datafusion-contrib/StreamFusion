# Fluss Arrow transport

The optional Fluss connector preserves the broker's columnar batches across the Java
boundary, then uses the existing Arrow C Data Interface bridge to Rust. It uses the
released Java client's connection and RPC implementations; it does not fork Fluss or
incorporate fluss-rust.

## Receive ownership

The accelerated connection replaces the released heap-preferring receive mode with
direct buffers. A frame-length-aware accumulator reserves complete frames instead
of repeatedly growing the generic direct cumulation. The pinned SDK initializer and
handlers remain intact; missing reflective transport access causes planner fallback.
The hook and version coupling are documented in the Fluss transport divergence.

An addressable direct RPC buffer is retained as an Arrow foreign allocation. The
allocator charges the complete allocation, while the IPC record batch takes slices of
its body. Closing the RPC response releases its original reference; closing the final
Arrow vector releases the retained allocation. Tests check addresses, reference counts,
allocator accounting and rejected reservations.

Uncompressed compatible vectors share the received body. Compression needs new
decompression buffers, timestamp representation conversion can allocate, and Rust's
alignment checks can require copying. Heap responses and remote-file reads use the
copying decoder. These boundaries prevent a claim that the full path is zero copy.

Before direct receive was enabled, an actual broker batch with 2,048 mixed-type rows was exported through the production
identity Calc and imported back into Java, with every value checked. Uncompressed
input retained 282,756 buffer bytes at the same addresses; 33,280 buffer bytes changed
addresses. LZ4 and ZSTD retained 315,524 bytes and changed 512 bytes after their required
decompression. These are buffer-address observations from a correctness smoke run,
not copied-byte counts or throughput results. That heap receive path had already copied
the Java Arrow body before this address check. They demonstrate why sharing must be
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
Sealed batches use the released SDK's sticky no-key assigner and cluster metadata.
Explicit client configuration beyond bootstrap servers retains the stock endpoint,
as does a statistics-enabled sink; see the [settings audit](../connectors/fluss-client-settings.md).
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

## Connection lifecycle

Concurrent readers and writers with identical complete configuration reuse one Java
connection through reference-counted leases. Independent IDs, sequences, split state
and queues stay with each owner. No idle connection survives its last lease, and final
shutdown remains synchronous. Cancellation releases a lazy RPC reply that arrives
after its awaiting reader is interrupted, even when other owners keep transport open.

Matched JFR source-task profiles show two serial approximately two-second Netty close
waits in the previous native source/sink chain. Reuse reduces native median q0 from
4.540 to 2.445 seconds and q14 from 4.955 to 2.910 seconds, with complete teardown
retained. Matched stock medians are 4.955 and 5.052 seconds. q20 improves from 12.522
to 3.440 seconds but has substantial control-run outliers. The connector page retains
every trial and configuration; the gains are lifetime improvements rather than a
claim that every query's steady-state throughput doubled.

The released SDK applies Netty's default two-second quiet period even when its
connection closes with `Duration.ZERO`. Immediate shutdown
uses the event loop exposed by the same already-validated bootstrap hook. Only the
last lease requests zero quiet time; the Java connection still closes and network
termination is awaited. Checkpoint and end-of-input acknowledgement flushes precede
normal sink close. Borrowed Arrow allocations retain their own references and remain
valid after network termination; a broker regression test checks that lifetime and
checks that the last close actually terminates the event loop. No idle cache or
asynchronous teardown is introduced.

Matched wall profiles capture approximately 2,004 ms in native q0's final lease close.
With immediate shutdown enabled, repeated release-mode RocksDB measurements (2M events,
parallelism four, mini-batching off) reduce native q0 median from 2.477 to 0.410
seconds (6.05×), and q14 from 2.934 to 0.923 seconds (3.18×). Stock medians remain
4.792/4.954 and 5.071/5.055 seconds, respectively. q20 changes from 10.206 to
4.077 seconds but has large outliers in both controls, so that entire reduction
cannot be attributed to shutdown. The [complete control samples](../benchmarks/fluss-close-controls.csv)
include every trial. These are lifecycle improvements in bounded jobs, not a claim
that sustained Arrow processing becomes six times faster. Immediate shutdown is now
the default within the opt-in connector after the 37 transport contract cases, all
10 ported SQL cases, and the complete 23-query
RocksDB correctness smoke passed. Set `-Dstreamfusion.fluss.fast-close.enabled=false`
to reproduce the SDK quiet-period control.

## Bounded compression adapter

LZ4 preserves the released Fluss block framing and compressor, adapting it to an
Arrow-backed output and a reusable 8 KiB scratch buffer. Compression no longer stages
the complete input in a heap array or grows/copies a complete heap output stream.
The temporary compressed-output allocation reserves the uncompressed input extent
plus frame overhead under the Arrow allocator; only written bytes enter serialization.
Decompression writes into its exact declared Arrow allocation and validates the final
length, with cleanup on malformed frames. Released block workspace allocations remain;
this is not zero-copy compression. NONE and ZSTD use their released implementations.

Direct receive held fixed, LZ4 append medians fall from 38.835 to 32.530 ms for narrow
input and from 264.357 to 178.357 ms for wide input (1.19×/1.48×). Wide decode falls
from 60.215 to 50.566 ms; narrow whole-read median regresses 6%, retained in the controls.
The complete frame-aware receive path improves wide read medians over generic direct
receive by 1.17×/1.16×/1.19× for NONE/LZ4/ZSTD, while narrow NONE whole read regresses
8%. Matched stock/current wide read speedups are 2.02×/1.69×/1.45× and append speedups
are 1.11×/2.18×/1.54×. [Every control trial and stage duration](../benchmarks/fluss-transport-controls.csv)
is retained; the connector page gives fixture configuration and the remaining copy boundaries.

Direct broker bodies now genuinely reach Java Arrow without heap staging. The JNI
identity check still copies under-aligned numeric buffers for uncompressed narrow
input (133,120 buffer bytes change addresses), whereas decompressed narrow numeric
buffers share 131,072 bytes. No complete broker-to-Rust zero-copy claim follows from
Java body borrowing alone.

The earlier memory-state 2M-event Nexmark validation has a 2.02× geomean of append-only
median speedups over matched stock Flink (16 queries, one warmup plus three pairs;
all teardown retained). q2/q20/q23 have substantial stock variance, and lifecycle
still dominates short jobs. All 23 runnable queries pass deterministic parity and
native-plan checks, with stock primary-key writers. This is end-to-end evidence for
the combined techniques, not attribution of the entire gain to receive or compression.
[Every final trial and configuration](../connectors/fluss.md#final-nexmark-validation)
is retained alongside the independent controls.

The earlier [RocksDB matrix](../connectors/fluss.md#rocksdb-nexmark-validation)
has a **2.34× append-only median-speedup geomean**, retaining full job costs
and all 55 measured pairs. It includes the Java-side repair of under-aligned
variable-width offsets before FFI import: Arrow-rs reads the last offset before
its native alignment pass. Data and validity remain borrowed; only offset buffers
that fail four-byte alignment are copied by this guard. Disk-state results do not
isolate the transport gains from the operators or the backend.

## Matched full-sweep validation

After zero-quiet synchronous shutdown, the matched RocksDB 16-query append-only
sweep has median-speedup geomeans of 8.56× for Fluss and 1.57× for Kafka. The
geomean of Kafka-native / Fluss-native medians is 2.12×. All job startup, flush,
checkpoint and teardown costs remain timed. These results combine engine and
transport work; stock stateful variance also affects the speedup denominator.
They do not attribute the full geomean change to shutdown. The [complete table and
192 samples](../connectors/fluss.md#final-matched-rocksdb-results-2026-10-07)
retain q23's slower Fluss median and all unfavorable outliers.

Separate warmed wall recordings verify that native q0's final lease-close scope
falls from 2,004 sampled milliseconds to 1, and q20's from 2,005 to 2. The stock
connector retains the quiet-period wait. These recordings confirm removal of the
fixed lifecycle penalty while the last-owner regression verifies completed network
termination and independent borrowed-vector lifetime.

A subsequent warmed q23 CPU profile is dominated by RocksDB (18,135 of 23,527
native samples; 50,912 of 75,995 stock samples). Its native fetch/decode/encode
scopes have 459/404/504 samples and compression has 820; scopes overlap. The
3.829-second profiled native execution passes parity but stays outside headline
metrics. It does not establish the location of every earlier outlier. No further
fixed networking delay or dominant copy scope appears in this recording. Remaining
SDK serialization, codecs and Arrow interoperability require broader changes;
join state work and skew are separate from the easy transport lifecycle fix.
