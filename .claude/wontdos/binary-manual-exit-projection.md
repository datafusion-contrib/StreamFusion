# Manual per-field BINARY exit projection

Reject the manual projection prototype tested on 2026-10-01. This decision applies
only to replacing Flink's generated projection with a Java per-field dispatch
loop; fixed-BINARY support and eliminating temporary payload arrays remain open
under [#235](https://github.com/datafusion-contrib/StreamFusion/issues/235).

The prototype wrapped Arrow payload buffers in batch-scoped Flink memory segments
and used the released byte-backed string writer to copy raw bytes into owned
binary rows. That writer shares the binary writer's inline/offset encoding and
performs no UTF-8 decoding. Existing row ownership and downstream copy behavior
were preserved. This followed Comet's owned binary boundary rather than emitting
borrowed row payloads.

29 targeted Flink 2.2.1 checks passed: five sliced-buffer wire/lifetime comparisons
at widths 1, 7, 8, 16 and 256; ten existing transpose ownership checks; fourteen
fixed-BINARY SQL parity checks. NULLs, invalid UTF-8, compact decimals, primitives,
all row kinds and copied rows retained after Arrow close were covered.

Release measurements used two million runtime rows, a blackhole sink, both
transposes, JNI, the unchanged native library, 1,024-row batches, a 2 GB JVM heap,
two warmups and five alternating measured trials. Source payload budget is 264
bytes; output BINARY width is 16. All trials and identity controls are retained:

| Projection / expression | Stock median (s) | Native median (s) | Native range (s) |
|---|---:|---:|---:|
| Manual / ELT | 0.305 | 0.402 | 0.396–0.408 |
| Prior generated / ELT | 0.303 | 0.387 | 0.379–0.395 |
| Manual / string cast | 0.570 | 0.615 | 0.607–0.627 |
| Prior generated / string cast | 0.551 | 0.613 | 0.599–0.618 |

The manual path does not demonstrate improvement over the prior native path and
still loses to stock. Removing an intermediate copy alone does not establish a
whole-job win. Per-field dispatch and type lookup are possible costs, but these
runs do not isolate their contribution. Keep Flink's generated projection and
investigate direct segment copying within generated field execution instead.
The prototype production code and tests were removed; implementation snapshots
and raw logs are retained with local experiment artifacts.

[Manual trials](../../docs/benchmarks/binary-direct-exit-candidate-2026-10-01.csv)
and [prior native trials](../../docs/benchmarks/binary-direct-exit-prior-native-2026-10-01.csv)
retain unfavorable timings. Shared-host variation prevents claiming a universal
regression; it does not provide evidence to accept the prototype.
