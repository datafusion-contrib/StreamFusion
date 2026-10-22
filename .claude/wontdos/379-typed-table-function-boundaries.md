# Exact and temporal table-function boundary prototypes

Two focused release experiments for [#379](https://github.com/datafusion-contrib/StreamFusion/issues/379)
were removed after profiling and complete-job comparisons on 2026-10-19. This rejects these
prototypes, not the verified type expansion. The maintainer explicitly accepted submitting
the coverage after two measured optimization passes, with the remaining slowdown recorded.

A 1 ms async-profiler CPU recording of a ten-million-row DECIMAL(38,18) projection includes
both engines. Of 14,401 samples containing the native pipeline entry, 4,537 include Arrow
writers, 4,414 include the exit transpose and 3,646 include RowDataSerializer. These are
inclusive, overlapping counts, not an additive cost breakdown. Decimal reconstruction and
BigInteger byte conversion also appear prominently. The profile uses the archived prior
release library; final unprofiled comparisons use the freshly built baseline library.

1. Allocate the output RowKind sidecar only on the first non-INSERT row, backfilling preceding
   INSERT tags. This follows the source transpose's existing policy. BOOLEAN improves in
   median, but trial ranges overlap and DECIMAL is unchanged. Allocation savings alone do
   not establish throughput improvement, so the prototype was removed.
2. Extend the existing generated binary-row exit projection to precision-19 through 38
   DECIMAL. Comet uses generated owned-row projection at the same boundary; this preserves
   Flink's current copied-output ownership contract. Two focused ownership checks pass,
   covering precision 19/38, scale zero/intermediate/full precision, signed extrema, NULLs,
   all RowKinds, two closed batches, and object reuse on/off. DECIMAL becomes slower, so
   both the prototype and its dedicated tests were removed. TIMESTAMP projection was not
   retried: its earlier rejected experiment is already recorded in
   [the temporal investigation](364-temporal-row-output-shortcuts.md).

Flink 2.2.1/JDK17, release+mimalloc, parallelism one, 2 GiB heap, 1,024-row batches,
two million runtime source rows, two warmups and three alternating measured pairs retain
both transposes, production IPC and the row sink. Own build processes were suspended during
timing. Separate JVM controls drift; these diagnostic medians do not isolate small changes.

| Prototype / projection | Stock median, s | Native median, s |
| --- | ---: | ---: |
| Initial / BOOLEAN | 0.285107 | 0.518845 |
| Lazy RowKind / BOOLEAN | 0.285750 | 0.497328 |
| Binary decimal exit / BOOLEAN control | 0.294557 | 0.553502 |
| Initial / DECIMAL(38,18) | 0.279629 | 0.720907 |
| Lazy RowKind / DECIMAL(38,18) | 0.260523 | 0.722861 |
| Binary decimal exit / DECIMAL(38,18) | 0.268925 | 0.748178 |
| Initial / TIMESTAMP_LTZ(9) | 0.324206 | 0.586108 |
| Lazy RowKind / TIMESTAMP_LTZ(9) | 0.292687 | 0.565915 |
| Binary decimal exit / unchanged LTZ control | 0.290223 | 0.551006 |

All warmups, measured arrays and the CPU sample summary are retained with the
[coverage benchmark artifact](../../docs/benchmarks/results/typed-table-functions-2026-10-19.json).
The [bounded follow-up](../../docs/optimizations/decimal-arrow-reads.md) for
[#425](https://github.com/datafusion-contrib/StreamFusion/issues/425) retains a decimal
read improvement. Further work is
[deferred with reopening criteria](425-remaining-table-function-conversions.md)
after that pass found no other simple optimization.
No speedup is claimed for the shipped whitelist expansion. Simple JVM functions can be
cheaper than two Arrow conversions even when their surrounding native operators benefit.
