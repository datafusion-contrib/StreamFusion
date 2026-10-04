# Generated BINARY segment-access projection

Reject the generated segment-access prototype tested on 2026-10-01. This is a
scoped implementation decision; [fixed-BINARY support #235](https://github.com/datafusion-contrib/StreamFusion/issues/235)
remains open, including its whole-job performance gate.

The prototype supplied binary field expressions through Flink's released
expression-generation API, retaining generated primitive/compact-decimal field
execution. A batch-scoped accessor wrapped Arrow buffers in Flink memory segments.
Private byte-backed-string expressions selected Flink's raw segment writer, whose
inline/offset encoding matches the binary writer without UTF-8 decoding. The
external BINARY/VARBINARY schema and owned-row/downstream-copy contract were
unchanged. Unknown vector layouts or buffers above Java ByteBuffer capacity used
the original projection. Borrowed references were cleared after each batch.

Five direct wire/lifetime checks passed on released Flink 2.2.1 for widths 1, 7,
8, 16 and 256, alongside fourteen fixed-BINARY SQL and ten existing ownership
checks. Direct comparisons included invalid UTF-8, NULLs, sliced buffers,
primitive/compact-decimal neighbors, every row kind and copied rows retained after
Arrow close. Cross-line prototype validation was not completed because the
performance gate did not establish sufficient benefit; no prototype code ships.

Release measurements retain two million runtime rows, source/blackhole sink,
both transposes, JNI, 1,024-row batches, a 2 GB JVM heap, two warmups and five
alternating trials. Output width remains BINARY(16):

| Input profile / expression | Stock median (s) | Prototype median (s) | Prototype range (s) |
|---|---:|---:|---:|
| 264-byte budget / ELT | 0.314 | 0.399 | 0.396–0.401 |
| 264-byte text / string cast | 0.567 | 0.606 | 0.598–0.609 |
| 4,096-byte text / string cast | 8.496 | 8.682 | 8.618–8.694 |

The prior native projection's 264-byte-budget samples are ELT 0.387 seconds and
cast 0.613 seconds, with different stock controls (0.303 and 0.551 seconds).
Those samples do not establish an ELT improvement or a clear broad advantage for
the prototype. All tested profiles still lose to same-run stock. The wide-input
identity control is also retained: stock 9.070 seconds versus native 10.345
seconds. An encoding-dominated win was a hypothesis; this run does not support it.
Do not attribute these differences to encoding or startup without isolated evidence.

The removed prototype adds batch access state, a second generated projection and
private encoding types without demonstrating enough whole-job benefit. Keep the
existing generated projection. Earlier CPU evidence places the Rust ELT leaf at
about 1% of task samples and conversion/copying above it; eliminating this
particular temporary output array has not overcome the measured perimeter cost.
Revisit only with a new profile or substantially different workload evidence.

[Small-input trials](../../docs/benchmarks/binary-generated-segment-candidate-2026-10-01.csv),
[wide-input trials](../../docs/benchmarks/binary-generated-segment-wide-candidate-2026-10-01.csv)
and [prior native trials](../../docs/benchmarks/binary-direct-exit-prior-native-2026-10-01.csv)
retain every measurement and identity control. Raw logs and implementation
snapshots remain in the local experiment archive.
