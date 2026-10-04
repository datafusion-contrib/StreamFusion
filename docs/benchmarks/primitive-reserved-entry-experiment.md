# Reserved primitive entry experiment

This isolated prototype follows Comet's Arrow writer pattern: when the entry
operator already reserves capacity for a bounded batch, fixed-width row fields
use non-growing setters. Numeric NULLs clear the validity bit without a capacity
check. Numeric arrays, complex fields and the general growing writer retain
checked writes. Primitive widening, math kernels, output ownership, batch limits
and row kinds are unaffected by this entry-only experiment.

The extra entry point requires pre-reserved capacity. Only the row-to-Arrow
operator invokes it, after reserving its known positive batch limit; it flushes
when that limit is reached. Calls without a positive batch limit keep growing
writes. Ordinary callers retain growth even when they supply an initial capacity.
This differs from increasing reserved capacity, which was previously measured
and rejected for other workloads.

Prepared tests cover all six numeric fields, NULLs, raw floating bits, integer
extrema, changing row kinds, upstream row reuse and retained values after three
batches close. A separate regression exercises the ordinary writer beyond its
actual initial capacity. The ownership, transpose, failure and growing-writer suite passes 32 checks
on each released Flink version (2.2.1 and 1.18.1), without skips. Eight math SQL
checks also pass on each version against the immutable fused library. The first
480-trial diagnostic compares TINYINT, nullable SMALLINT and DOUBLE small-range
profiles in candidate/original/original/candidate order, with matched stock
controls. It keeps both transposes and a rowwise blackhole, ten million rows,
two warmups and five repeats per engine, two CPUs and a 2 GiB heap. No heavy
work overlaps timing. This initial matrix is not complete acceptance coverage.
All twelve blocks completed successfully, providing 480 trials and twelve
summary rows. Independent validation confirms source hashes, loaded-library
witnesses and restoration of candidate entry sources. TINYINT COSH, SINH and
TANH medians improve by 1.27%, 0.92% and 1.46% against the previous fused
prototype, respectively. Nullable SMALLINT instead regresses by 0.38%, 1.63%
and 0.67%. SMALLINT TANH remains 9.50% slower than its matched stock control;
TINYINT TANH remains 5.44% slower. These mixed results do not support admission.
The DOUBLE control changes COSH by -0.73%, SINH by +0.77% and TANH by
-0.13% against the previous prototype. All trial ranges remain retained.
This comparison changes entry writers only; its previous native baseline is the
fused math prototype, not the shipping fallback. The isolated reserved-write technique is rejected for production admission:
its small, mixed effects do not resolve typed math regressions.

[All 480 trials](primitive-reserved-entry-trials-2026-10-04.csv) and
[12 summaries with ranges](primitive-reserved-entry-summary-2026-10-04.csv)
retain matched stock controls. Reproduce from be309d83 by applying the
[baseline](strict-hyperbolic-baseline-prototype.patch),
[typed benchmarks](strict-hyperbolic-typed-benchmarks.patch),
[fusion](strict-hyperbolic-fusion-prototype.patch), then the
[entry and ownership tests](primitive-reserved-entry-prototype.patch).
These patches are experimental and are not enabled production code.
