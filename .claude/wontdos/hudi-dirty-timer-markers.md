# Per-timer dirty markers for persistent window firing

Rejected after the first deadline-cache optimization. Keeping a dirty bit beside
each timer avoided rewriting already persisted entries, but the focused Hudi Q5
comparison did not establish a stable complete-job improvement that justified
the extra metadata and lifecycle handling.

At 200k events, four source/sink tasks, RocksDB and mini-batching off, one warmup
and three rotating trials gave native medians of 10,039 ms before and 8,578 ms
after. Their ranges overlap: 8,528–10,479 versus 8,577–10,333 ms. The Hudi-disabled
path regressed from 7,635 to 8,530 ms; stock moved from 1,289 to 1,325 ms.
Correctness, encoder and native cleanup checks passed, as did 100 affected native
tests. The remaining profile is dominated by iterator work, so the next approach
uses existing deadline metadata to seek past expired history.

[The candidate patch](hudi-dirty-timer-markers.patch) applies to `f5e5fd566` and
is retained for reproducing this decision; it is not part of the implementation.
Structured trial observations and source provenance are retained with the
[window-firing report](../../docs/benchmarks/hudi-window-firing-20261114.json).
Reconsider this additional tracking only with a repeatable representative gain.

The initial [per-group seek variant](hudi-per-group-window-seek.patch), also based
on `f5e5fd566`, was inconclusive: native 9,137 ms (7,662–9,257) versus the same
10,039 ms baseline, and Hudi-disabled SF 8,192 versus 7,635 ms. Its replacement
reuses one cursor across populated groups; the report retains both measurements.
