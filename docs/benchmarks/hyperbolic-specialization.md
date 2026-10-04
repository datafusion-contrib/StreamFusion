# Hyperbolic compiler specialization diagnostic

The isolated prototype takes a concrete function item rather than a function
pointer inside the primitive loop, allowing compiler specialization. Arithmetic,
numeric widening, NULLs, output schema and ownership remain unchanged. This is
experimental evidence; production math admission is unchanged.

All 816 frozen Java 17 oracle fixtures and allocation probes pass with release/
mimalloc and verified malloc aliases. All 816 allocation rows match the previous
fused prototype. Eight runtime SQL checks pass without skips on each released
Flink version, 2.2.1 and 1.18.1.

The interleaved candidate/original/original/candidate Criterion comparison covers
five typed inputs, all three functions, 16384 non-NULL rows and offset zero.
Each case has one-second warmup, one-second measurement and twenty samples.
[60 block estimates](hyperbolic-specialization-criterion-blocks-2026-10-04.csv),
[15 comparisons with block ranges](hyperbolic-specialization-criterion-summary-2026-10-04.csv)
and [1200 raw samples](hyperbolic-specialization-criterion-samples-2026-10-04.csv)
are retained. Integer TANH improves 11.92–14.11%; COSH improves 5.92–7.30%
across the five types. Integer SINH improves 2.43–3.72%. FLOAT TANH improves
1.49%, while FLOAT SINH regresses 1.09%. This first timing matrix does not
establish performance across NULLs, other sizes or slices.

[All 1680 full-job trials](hyperbolic-specialization-trials-2026-10-04.csv) and
[28 summaries with all ranges and stock controls](hyperbolic-specialization-summary-2026-10-04.csv)
compare specialization, previous fused native behavior and shipping fallback in
candidate/previous/shipping/shipping/previous/candidate order. Each block uses
ten million rows, two warmups, five repeats per engine, Java 17, Flink 2.2.1,
two CPUs and a 2 GiB heap. Both transposes, disabled local zero-copy exchange,
release/mimalloc and a rowwise blackhole remain measured. No heavy work overlaps
timing. All 42 blocks pass; independent validation verifies Maven results, load
witnesses, source/library guards and restoration of the candidate planner.

The kernel gains mostly become sub-percent whole-job changes. Integer TANH
remains 4.69–8.84% slower than shipping and 5.26–10.63% slower than stock.
All BIGINT and FLOAT functions remain slower than both controls. Small-range
DOUBLE beats stock by 5.85–8.20%, and nullable larger-range DOUBLE by
4.62–6.11%, but specialization changes previous-native DOUBLE times by only
-0.35% to +0.24%. These results do not justify broad primitive admission or
complete the floating-function work. Keep the shipping fallback while further
performance work addresses conversion and ownership costs.

To reproduce from be309d83, apply the [baseline](strict-hyperbolic-baseline-prototype.patch),
[typed benchmarks](strict-hyperbolic-typed-benchmarks.patch),
[fusion](strict-hyperbolic-fusion-prototype.patch), then
[specialization](strict-hyperbolic-specialization-prototype.patch). Do not apply
the rejected reserved-entry or generic-exit experiments. The patches preserve
experimental code rather than enabling known slower cases in production.
