# Fixed-width entry capacity as a standalone ELT speedup

The updated ten-million-row ELT profile identifies fixed-width allocation/initialization and
safe writes at the entry boundary. Comet's `RowArrowReader` and `ArrowWriters.create` were
consulted before design: fixed-width vectors allocate for the batch limit, while other vectors
keep their default policy and each batch owns independent buffers.

A removed prototype added a writer capacity overload and passed the 1,024-row limit from the
entry operator. It sized only top-level fixed-width buffers, leaving variable/nested allocation
unchanged. Its additional fixture demonstrated smaller fixed buffers, unchanged variable/list
capacity, safe growth beyond the hint, NULLs and ownership under reused binary/array input.
All 68 existing Flink 2.2.1 transpose/ownership/SQL checks and the new fixture pass. Flink 1.18
prototype validation was not run because the measured speedup did not survive the control.

Release/mimalloc, JDK 17, Flink 2.2.1, 2 GiB heap, ten million non-null source rows, two warmups
and five alternating trials retain both transposes, native Calc and the rowwise blackhole sink.
ELT's index cycles through 0–3, yielding NULL for invalid indices. The prototype median is
1.254910 seconds (1.246681–1.261413), against stock 1.012933 (0.997792–1.121131). Fresh
unchanged production takes 1.259936 (1.255633–1.271457), against stock 1.017267
(1.006969–1.043799). The 0.4% native difference overlaps variability and stock drift; both
remain slower than stock. Identity trials are retained alongside all function trials.

The prototype and its test were removed. Lower reserved memory is demonstrated by the fixture,
but it does not establish a representative performance gain or resolve ELT admission. Revisit
only with a workload or profile that establishes the allocation policy as a limiting cost.
See the [entry ledger](../../docs/optimizations/projection-pruning-transpose.md) and
[all trials](../../docs/benchmarks/fixed-entry-capacity-2026-10-02.csv).
