# Reserved primitive row entry

Rejected as an isolated production optimization on 2026-10-04. Follow Comet's bounded Arrow writer pattern to replace checked primitive setters only after
reserving the batch capacity. Forty correctness checks pass on each released
Flink version, including ownership and ordinary growing-writer tests.

The 480-trial release/mimalloc complete-job comparison finds TINYINT math
improvements of 0.92–1.46%, nullable SMALLINT regressions of 0.38–1.63%, and
DOUBLE effects from -0.73% to +0.77%. Typed TANH remains slower than stock.
Keep the current entry path. Do not retry this unchanged technique; revisit
only with a changed workload or evidence of significant checked-setter cost.

The portable prototype, all raw trials, ranges, and methods are preserved in
`docs/benchmarks/primitive-reserved-entry-experiment.md` and its linked artifacts.
The broader floating-function and Criterion coverage goals remain pending.
