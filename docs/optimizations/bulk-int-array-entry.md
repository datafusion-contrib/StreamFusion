# Bulk entry writes for INT arrays

INT arrays with more than one element reserve child-vector capacity once before
writing their values. Each element then uses the reserved range, and the child
writer count advances once per array. Empty and singleton arrays retain the
existing per-element loop. This reduces writer dispatch and capacity checks; it
does not eliminate payload copies or claim lower allocation.

Parent list offsets, child NULL masks, nested arrays and fresh-batch ownership
retain their existing behavior. Exported buffers remain retained and a fresh
batch never overwrites them. Comet's Arrow writer pattern was consulted before
this change. Ownership, transpose, failure-cleanup and runtime ARRAY_DISTINCT SQL
checks pass 41 cases without skips on both released Flink 2.2.1 and 1.18.1, using
the exact delivery Java sources and canonical release/mimalloc library.

## Whole-job evidence

The final five comparisons use already-supported INT ARRAY_DISTINCT and a native
library matching canonical production Rust, with no experimental array-cast
implementation. Each candidate/original/original/candidate block starts a fresh
JVM, performs two warmups and five alternating stock/native repetitions. Runs
within a block share the JVM. Both row/Arrow transposes and the rowwise blackhole
remain measured: Java 17, Flink 2.2.1, two CPUs, a 2 GiB heap, release/mimalloc,
local zero-copy exchange disabled. No heavy work overlaps timed trials. Only the
three entry-writer files vary. Times are pooled medians in seconds; negative
percentages mean faster. NULL patterns give parent/element periods, with zero
meaning no NULLs. The empty fixture explicitly verifies zero-length arrays; its
domain and element-NULL settings have no effect on empty contents.

| Width | Rows | Domain | NULL pattern | New native | Prior native | Stock | Versus prior | Versus stock |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 10,000,000 | 8 | 8/7 | 1.406787 | 1.415357 | 1.342771 | -0.61% | +4.77% |
| 1 | 10,000,000 | 65536 | 0/0 | 1.510447 | 1.504536 | 1.534712 | +0.39% | -1.58% |
| 8 | 10,000,000 | 65536 | 8/7 | 2.059181 | 2.116566 | 2.785315 | -2.71% | -26.07% |
| 64 | 2,000,000 | 65536 | 8/7 | 1.842782 | 1.972906 | 9.184806 | -6.60% | -79.94% |
| 64 | 2,000,000 | 8 | 8/7 | 1.098850 | 1.338459 | 2.703348 | -17.90% | -59.35% |

The width-eight and both width-64 native timing ranges do not overlap their
previous native controls. Singleton results are near level with the previous
writer (overlapping ranges), rather than an established improvement. Empty
arrays also remain near level with the previous path and still lose to stock;
this change preserves their existing loop and does not claim to accelerate them.

[All 560 trials](../benchmarks/int-array-entry-trials-2026-10-04.csv) and
[all 56 engine summaries](../benchmarks/int-array-entry-summary-2026-10-04.csv)
retain the original unbounded prototype, the revised writer with the experimental
library, and the final canonical-library comparisons, including all unfavorable
trials and timing ranges. The initial unbounded writer regressed singleton
ARRAY_DISTINCT by 1.97%, prompting the small-array dispatch rule. Measurements on
the experimental library are labeled separately and do not replace the final
canonical-library evidence.

This improves an existing native operation; it does not change collection
function admission. ARRAY<INT> to ARRAY<BIGINT> casts remain on Flink because the
separate cast experiments regress sustained small-array jobs. Issue
[#234](https://github.com/datafusion-contrib/StreamFusion/issues/234) remains
partial. Wider integer element types and other operations need their own
performance evidence before extending the bulk writer.
