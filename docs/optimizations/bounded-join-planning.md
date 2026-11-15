# Bounded join and subplan planning

The bounded planner follows DataFusion's cross-join elimination for a narrow
physical pattern: `(A × B) JOIN C` with equality links from both dimensions to C.
When both dimension estimates are no larger than C, it can build each dimension
and probe C through two linked joins instead of materializing the Cartesian
intermediate. A projection and, when required, blocking Arrow exchange restore
the original schema and distribution. Hints, residuals, NULL-safe equality and
ordered joins retain the original plan. The SQL remains unchanged.

This allowed TPC-H Q9 at scale factor one to finish within the original 512 MiB
native authority. Its repeated release comparison measured 27,230.35 ms stock
versus 1,919.77 ms native (14.184×). A separate regression-check cohort measured
Q2 at 3,427.85 ms stock versus 535.95 ms native (6.396×), replacing an earlier
native Cartesian plan that lost to stock. These shared-host cohorts include
full scans, transposes, JNI, blocking shuffles, sinks and completed job results;
their variability and earlier failures remain in the [TPC-H evidence](../benchmarks/tpch.md).

The planner also preserves Flink's reuse of a shared unary aggregate subplan.
It runs the host subplan reuser before native substitution, counts consumers of
the admitted shared aggregate, and preserves that subtree's digest through
Flink's clone-and-reuse cycle. The existing native share operator then gives
each branch a retained Arrow buffer view. Other native nodes retain their
single-consumer digest barriers. Both host reuse switches and native source
sharing must be enabled. Scheduling, replay and memory accounting remain with
their existing owners; no result cache is introduced.

Sharing reduced Q15's native median from 1,393.73 ms to 794.67 ms in separate
cohorts, but the matching stock median was 596.21 ms. This remains a regression
to optimize and is not claimed as acceleration. The complete 22-query goal
remains open while its performance and validation work is unfinished.
