# Adaptive small-set membership for wide ARRAY_DISTINCT

Rejected on 2026-10-02 for the measured general STRING path. Keeping eight distinct
values on the stack and promoting on the ninth saves repeated wide-string hashing
for low cardinality, but all three tested fingerprints regress non-null unique
strings. The final comparison reports +8.45% (3.26–14.26%), despite roughly 56%
improvement in duplicate-heavy fixtures. All correctness probes pass; the problem
is the workload tradeoff, not equality semantics.

Production code and the prototype-only test were removed. Do not repeat the same
cardinality speculation without a materially different strategy or new evidence.
CPU profiling attributes only about 10% of native task samples to this kernel;
Flink source conversion and the entry transpose remain the larger boundaries.

See docs/optimizations/scalar-function-kernels.md for method and all estimate CSVs.
This excludes only these adaptive membership variants, not native collection
acceleration or future state/layout improvements. Issue #234 stays open:
https://github.com/datafusion-contrib/StreamFusion/issues/234.
