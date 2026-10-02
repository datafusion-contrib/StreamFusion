# Reusing the window-state value serialization buffer

**Status:** measured candidate rejected, 2026-10-02. Persistent-state optimization
work remains open; this decision closes no broader issue.

The candidate reused one owned byte buffer within each window-state write call,
clearing and reserving it before encoding the window-start prefix and state row.
RocksDB's released write-batch API copies each value synchronously; no borrowed
buffer escaped. The encoded format and dependencies were unchanged. This removes
per-value allocations, while retaining the required serialization and write-batch
copies. Production retains its original per-value allocation.

Release Rust state tests passed 104 cases, with two profiling tests ignored.
The retained regression test checks short/long values, Unicode, NULs, NULLs,
overwrites, window-start prefixes and forced write-batch flushes. All 1,728
Criterion fixtures passed in the candidate and restored-original runs. Requested
Rust bytes fell in 24 candidate profiles and were unchanged in 1,704; output
buffer counts matched. In the wide unique-key nullable 16,384-row update,
14,895 allocation requests and 253,215 requested bytes were saved (about 0.47%
of the original requested bytes). These counters exclude C++ and worker threads.

Eight 16,384-row update shapes were measured sequentially as original, candidate
and restored-original control. Each used 100 samples, three seconds of warmup
and a five-second measurement target, with the System counting allocator.
The full untimed fixture/oracle prelude ran before each selected timing, source
hashes were checked at completion and no heavy local workload overlapped timing.

| Case | Original, ms | Candidate, ms | Restored original, ms |
| --- | ---: | ---: | ---: |
| Repeated short non-null keys | 0.949 | 0.413 | 0.411 |
| Repeated wide nullable keys | 1.287 | 1.616 | 1.741 |
| Unique short nullable keys | 29.424 | 28.529 | 28.399 |
| Unique wide nullable keys | 52.032 | 53.483 | 52.960 |

The large repeated-key differences also appeared in unchanged production, so
neither the apparent 56.5% gain nor 25.6% loss against the first run establishes
a candidate effect. Unique-key candidate/control differences were within 2% and
mixed. The repeated short nullable case was 6.1% slower than control. No broadly
repeatable throughput improvement was established; causes of timing drift remain
unproven. Original/control diagnostics differed in one 1,024-row case by one
allocation/568 bytes, with identical output counts; all other cases matched.

No candidate release/mimalloc whole-job improvement or released Flink parity was
established. Revisit only with a new hypothesis and representative matched
whole-job evidence. Estimates, confidence intervals, all 2,400 timing samples
and the allocation comparison are retained in
[the benchmark record](../../docs/benchmarks/native-criterion.md#window-state-value-buffer-reuse-experiment).
