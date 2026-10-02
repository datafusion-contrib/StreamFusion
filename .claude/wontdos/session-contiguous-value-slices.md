# Contiguous session value slices

**Status:** measured candidate rejected, 2026-10-02. No broader issue is closed.

After restoring arrival order, the candidate used owned Arrow slices for
contiguous session selections and retained indexed `take` for scattered rows.
This followed existing owned slicing patterns in Comet and session restoration;
it changed no JNI ownership, dependencies, state format or operator architecture.
Production retains its original selection loop.

Both new regressions and all 16 selected release session tests passed with the
candidate. They cover sequential FLOAT cancellation, nullable Unicode/NUL string
DISTINCT, sliced input offsets, singleton and scattered selections, input release,
raw snapshot restoration and no refiring. An initial sequential DOUBLE assertion
failed identically in original and candidate: Arrow's aggregation is not the
explicit sequential FLOAT fold. That failure proves no candidate regression and
no arbitrary DOUBLE parity.

Original, candidate and restored-original Criterion runs each passed all 1,848
unique diagnostics. Five phases used 100 samples each, three-second warmups and
five-second measurement targets, with untimed per-iteration fresh database setup.
Fixture validation interleaves with benchmark registration. All six guarded source
hashes matched original/control; only the session selection source differed in the
candidate. No heavy local workload overlapped timing.

Wide nullable unique-key merge saved 74,470 Rust allocation requests and 2,740,496
requested bytes. Across all profiles, 14 had fewer allocation calls and 1,834 were
unchanged, with identical output counters. Counters exclude C++ and other threads
and do not measure copied bytes or peak memory.

Merge means were 291.554/290.088/229.110 ms for original/candidate/control. There
was no repeatable merge benefit. Fire means were 57.409/35.085/58.016 ms and
checkpoint means 12.693/72.652/9.958 ms; differences in these other phases do not
establish a causal slicing effect. Restore differences were small and mixed.
All 1,500 samples and confidence intervals are retained. Two unchanged-source
control diagnostics differed by one allocation/56 bytes; all output counters
matched. The cause of timing drift is unproven.

No candidate release/mimalloc whole-job improvement or released-Flink parity was
established. Revisit only with a new performance hypothesis and representative
matched whole-job evidence. Full estimates, samples and allocation comparisons
are in [the benchmark record](../../docs/benchmarks/native-criterion.md).
