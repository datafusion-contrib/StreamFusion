# Block-based transpose timing sampling

A local TPC-H candidate chose one random conversion-time sample per 64 rows,
avoiding one random draw per row while keeping the metric's sampling weight.
It was reverted after three release cohorts failed to establish a consistent
complete-job improvement against matching stock Flink on Q6/Q14/Q15. Host load
varied substantially. The original sampler remains unchanged; this is not a
reason to remove conversion metrics or to weaken the full-job benchmark boundary.

[All trials](../../docs/benchmarks/results/tpch-rejected-timing-sampler-20261110.json).
A future change needs stable profiling and repeated evidence rather than assuming
that saving random-number calls improves these jobs.
