# Bind native LZ4 to each StreamFusion task's input gates

Comet's LZ4 codec explicitly uses `LZ4Factory.nativeInstance().safeDecompressor()`
and validates encoded lengths. StreamFusion follows that selection and ownership
pattern while retaining Flink's block format and buffers.

Released Flink 2.2 selects its block codec from a closed compression enum. It has
no public setter for replacing only the decoder on an existing input gate. Replacing
the entire shuffle service would duplicate or wrap substantially more scheduling,
buffer, IO and recovery behavior for a codec change.

The narrow compatibility bridge unwraps Flink's `InputGateWithMetrics.inputGate`,
then accesses `SingleInputGate.bufferDecompressor`
and `BufferDecompressor.blockDecompressor` on the task being initialized. Both supported
Flink lines have these fields. It replaces only the stock LZ4 implementation and
preserves the outer buffer decompressor. It neither changes a global LZ4 factory nor
patches/shadows a Flink class or JAR. Unknown layouts and unavailable JNI retain the
original decoder and log the limitation. Tests check framing, malformed inputs,
idempotency, instance isolation, other codecs, actual SQL execution and the disabled
control on both supported Flink lines.
