# Native LZ4 shuffle decompression

StreamFusion tasks use the bounds-checked native LZ4 decoder for Flink's compressed
shuffle blocks. Flink retains its network buffers, block framing, scheduling, disk IO
and recovery protocols. Compression is unchanged. Stock tasks keep Flink's decoder.

Flink 2.2.1 calls `LZ4Factory.fastestInstance().fastDecompressor()`. Its released
`at.yawk.lz4:lz4-java:1.10.3` dependency intentionally returns the pure Java decoder
from this legacy API, including for the JNI factory. The native implementation is
available through `nativeInstance().safeDecompressor()`, which accepts compressed
input length and bounds the output. StreamFusion uses that API and checks the
decoded length against Flink's header. It never selects the insecure factory.

This follows Comet's `Lz4FrameCompressionCodec` precedent: explicitly request the
native factory, use the safe decompressor, and validate lengths before touching the
payload. StreamFusion preserves Flink's existing eight-byte block header rather
than introducing Arrow frame compression. Heap arrays and direct buffers retain
their storage. A read-only heap view needs a reusable compressed-input copy because
it hides its backing array from JNI.

The decoder is installed on the task's existing input gates during StreamFusion
operator setup, before normal input processing. Native allocator initialization
also covers native writers that inherit a connector's operator. The union schema
adapter installs it before reading input. The compatibility bridge checks the
existing codec type and changes only LZ4. Repeated installation is idempotent;
unrelated tasks, decoder instances and other compression codecs are unchanged.
Flink 1.18 and 2.2 are tested. Missing JNI support or unavailable runtime fields
retain Flink's decoder with a warning; supported benchmark runs reject fallbacks.
The union schema and shared-subplan adapters pin their existing Java serialization
identities, so lifecycle hook changes preserve deployed operator descriptors and
configuration fields. The unchanged compatibility inventory checks all 73 operators.
The [bridge decision](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/task-local-native-lz4.md) explains why it
uses task-local runtime fields instead of replacing Flink's shuffle service.

`streamfusion.shuffle.nativeLz4.enabled` defaults to true in TaskManager
configuration. A JVM `-Dstreamfusion.shuffle.nativeLz4.enabled=false` override keeps
the preceding decoder for diagnostics. This is a TaskManager/JVM setting, rather
than a SQL planner admission or cost rule. Metrics record `nativeLz4InputGates`,
`nativeLz4Blocks`, `nativeLz4Bytes` and `nativeLz4FallbackGates`; successful native
decodes alone increment the block/byte counters.

The [TPC-DS harness](../benchmarks/tpcds.md) supports a matched three-engine
comparison of stock Flink, preceding StreamFusion with Java shuffle decoding, and
StreamFusion with native LZ4. It retains all production conversions and commit costs.
Component compatibility checks and positive native execution do not by themselves
establish an end-to-end speedup; measured follow-up cohorts are recorded separately
from the original Java-decoder results.

The optional `CI startup profile` workflow's `suite=native-lz4` runs only the
eleven released-codec compatibility invocations on the selected Flink/JDK line.
It needs no StreamFusion Rust build: only this codec-only job disables the global
shared-cluster extension that checks Rust handles. Normal SQL and merge suites
retain that extension and their existing assertions.

The complete matched SF1 comparison on Flink 2.2.1/Paimon 2.1.0 passes all 103
variants and 3,090 committed-result checks. Per-query median geometric speedups
are **1.1141× over the same-source Java decoder** and **2.2823× over stock Flink**.
JNI wins 90/103 versus the Java control and 100/103 versus stock; Q88, Q90 and Q96
remain slower than stock. All 13 losses versus Java and every trial range remain
in the [dated comparison](../benchmarks/tpcds.md#native-lz4-matched-comparison-2026-11-15).
The cohort records 10,324,169 actual JNI block decodes and zero decoder fallbacks.
These measurements retain IO, production Arrow IPC and Paimon commits on a shared
Apple M4 host; they do not establish a uniform per-query gain or a legacy-line
performance result. Both supported Flink lines pass the codec, SQL and recovery
checks; the focused Linux check independently passes all eleven codec invocations.
