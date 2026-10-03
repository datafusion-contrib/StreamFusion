# Moving missing RocksDB keys into the working set

**Status:** measured candidate rejected, 2026-10-02. Persistent-state optimization
work remains open; this does not close any broader issue.

The candidate consumed the already-owned missing-key list in batch loading,
moving keys into fetched-state decoding or absent working entries instead of
cloning them. TTL companion cleanup retained its required second-owner clone.
It added no hash lookup, dependency or state representation change. The
production implementation has been restored.

Release Rust RocksDB checks passed 103 tests, with two profiling diagnostics
ignored. Across all 1,608 Criterion allocation fixtures, requested Rust bytes
fell in 24 GROUP ingestion cases and were unchanged in 1,584; output-buffer
bytes were unchanged throughout. This excludes C++ and background allocations.

The eight ingestion timings used 16,384 rows, repeated/unique key domains,
short/wide UTF-8 keys and both nullability modes. Each run used three seconds
of warmup, 100 samples and at least five seconds of measurement, sequentially
original, candidate and restored-original control. The benchmark used the
System counting allocator rather than production mimalloc. All untimed
fixtures and output oracles ran before filtered timings; source hashes were
checked at completion. No concurrent local build or profile ran during timing.

| Case | Original, ms | Candidate, ms | Restored original, ms |
| --- | ---: | ---: | ---: |
| Repeated short non-null keys | 1.585 | 3.133 | 1.576 |
| Unique wide nullable keys | 46.337 | 53.456 | 45.788 |

These controls do not establish the regression mechanism, but they contradict
accepting the allocation saving as a throughput improvement. Java parity and
release/mimalloc whole-job performance were not established for the candidate.
Revisit only with a new explanation and matched measurements, retaining these
unfavorable controls. Full estimates, confidence intervals, samples and allocation
comparisons are linked from [the benchmark record](../../docs/benchmarks/native-criterion.md#persistent-key-ownership-experiment).
