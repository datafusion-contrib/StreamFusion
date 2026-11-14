# Avoiding the Arrow owner-registration monitor

Rejected after the isolated Q8/Q12 comparison in
[run 43619138937](https://github.com/jordepic/StreamFusion-contrib/actions/runs/43619138937).
The five-line volatile fast path avoided taking the registration monitor after
the shared Arrow owner was initialized, but did not establish a stable complete-job
Q8 improvement that justified retaining another lookup path.

At 200k events, four source/sink tasks, heap state and mini-batching off, two paired
passes alternated baseline/candidate then candidate/baseline. Each invocation used
one warmup and three rotating stock/previous/native release trials. Six Q8 native
timings per variant gave medians of 5,832 ms before and 5,894 ms after; ranges
overlap (5,567–6,159 versus 5,662–5,943 ms). Previous SF moved 6,109 -> 6,141 ms;
stock moved 1,190 -> 1,312 ms. This does not establish a regression either.

Q12 also included the independent processing-time batch proof and improved
6,121 -> 400 ms. That combined result does not attribute a gain to owner lookup.
All count/full-value, encoder, resource cleanup and source/library checks passed.
The retained batch proof is rechecked after removing this allocation candidate.

[All 72 observations and provenance](../../docs/benchmarks/hudi-java-comparison-20261114.json)
and [the patch against `3da6a654d`](hudi-arrow-owner-lookup.patch) remain available.
Both the candidate's production change and its concurrency-specific test were
removed. Reconsider only with a stable representative pipeline gain, including a
matched Mac comparison if the earlier monitor-heavy Mac profile is revisited.
