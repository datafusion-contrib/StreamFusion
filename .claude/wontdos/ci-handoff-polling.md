# Keep the canonical handoff readiness interval

The November 12, 2026 paired probe compared a one-second filesystem-watch timeout
with 100 ms. Readiness counters and task states can advance without another file
creation, so a shorter fallback looked promising. Both variants retained the same
readiness predicates, deadlines and checkpoint assertions.

All 70 invocations passed on Flink 2.2, including metadata preparation on JDK 11
and 17. Three alternating pairs gave a 1.3% elapsed median improvement on JDK 11
and a 1.5% regression on JDK 17; CPU increased slightly on both. The gain is small
and inconsistent, so keep the existing one-second wait and omit the experimental
configuration switch from the implementation.

The [structured trials](../../docs/benchmarks/ci-profile-20261112.json) retain the
measurements and source/run references. Revisit only with evidence that readiness
waiting materially dominates a representative workload.
