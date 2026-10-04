# Narrow primitive Arrow-backed exit

Rejected on 2026-10-04. CPU and allocation profiles for TINYINT TANH and
nullable SMALLINT COSH motivated bypassing the generated binary-row projection
for at most four primitive fields. The prototype passes the reusable
Arrow-backed row to Flink's normal ownership copy; explicit object reuse still
copies through the existing serializer. Comet's columnar-to-row projection and
Flink's binary versus generic row serializer were consulted before design.

Ownership, SQL parity, transpose and failure tests pass 39 checks without skips
on each released Flink version, 2.2.1 and 1.18.1. Tests cover both reuse modes,
NULLs, floating raw bits, extreme values, row kinds and retained rows after
successive input batches close. Correctness does not establish acceleration.

The 480 retained trials compare candidate/original/original/candidate blocks for
10 million TINYINT, nullable SMALLINT and DOUBLE rows. Both native alternatives
use the same experimental hyperbolic kernels and planner admission; the original
is the projected native prototype, not shipping Flink fallback. Java 17, Flink
2.2.1, two CPUs, a 2 GiB heap, release/mimalloc, both transposes and a rowwise
blackhole are held fixed. Each block has two warmups and five measurements per
engine, with matched stock controls and no overlapping heavy work.

All nine mathematical comparisons regress against the previous native exit,
by 10.45–14.54%. Identity controls regress too. Stock controls also move
substantially between blocks; this variation prevents attributing every change
to the exit implementation, but does not establish a performance win. Do not
normalize away the variation or use selected matched-stock wins to admit it.
The original exit and prototype-only ownership test have been restored.

Allocation profiles also show the exit's own temporary StreamRecord contributes
only about 0.01% of weighted sampled allocations. Most remaining record carriers
come from Flink's downstream ownership copy. Reusing the local record therefore
has no evidence of addressing the measured bottleneck.

The [benchmark record](../../docs/benchmarks/native-criterion.md#narrow-primitive-exit-rejection)
links all trials, summaries and both reproducible prototype patches. These
patches are experimental evidence, not production admission. Issue
[#236](https://github.com/datafusion-contrib/StreamFusion/issues/236) remains open;
this rejects one exit technique, not the requested floating-function work.
