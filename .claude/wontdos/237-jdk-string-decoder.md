# Keep Flink's external String decoder

Profiles of the wide JVM table-function split case placed about 23% of sampled table-function CPU
in released Flink's UTF-8 decoder. A replacement standard StringStringConverter subclass used the
JDK String(byte[], offset, length, UTF_8) constructor for single heap segments, preserving Flink's
cached Java String, binary bytes, internal conversion and all fallback cases. It replaced only
exact standard-converter references in generated correlate operators.

Correctness passed on released Flink 2.2.1/1.18.1 and JDK17: exhaustive two-byte inputs, randomized
longer inputs, malformed Unicode boundaries, cached identity/comparison, offsets, long strings,
off-heap/multiple segments, serialization, table-function recovery and transpose ownership.

Reject it. Inclusive sampled table-function allocations rise from 10.623 to 11.621 GB on 1.18 and
10.683 to 11.769 GB on 2.2. External-converter allocations rise from 1.250 to 2.305 GB and 1.222 to
2.382 GB respectively; CPU stacks move into JDK String construction. Both recordings observed
external activity, so no timing or causal CPU speed claim is made. Source snapshots, all Java/Rust
stacks, control profiles and raw logs are retained in
`docs/benchmarks/results/jvm-table-rejected-decoder-2026-10-16.json`.

The replacement and its production hook are removed. Continue from the measured UTF-8 encoding
cost with released decoding intact. This rejects one JDK17 decoder substitution; #237 remains
open for its implementation and performance requirements.
