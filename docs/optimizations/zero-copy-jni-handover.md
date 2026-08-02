# Zero-copy batch handover across JNI

**Applies to:** every batch crossing the Java↔Rust boundary

Batches cross the Java↔Rust boundary via the Arrow C Data Interface — the consumer reads the
producer's memory in place, no serialization, with ownership transferred so buffers are released
exactly once.

## Batching amortizes the boundary

Rows are buffered into batches before crossing into native code, so the JNI cost is paid once per
thousands of records instead of per record. This is what makes the crossing pay off at all versus
per-row execution.
