# Keep the existing JDK17 collector for canonical workers

The November16,2026 separate-host Kafka observations suggested roughly100s per
class with Serial versus140s with G1. They were a profiling lead, not a causal
speedup. Run43904169829 compared both collectors on the same runner per Flink
line: one warmup pair followed by three alternating measured pairs, pinned
JDK17.0.21+9, identical debug-native libraries, four original Kafka failure
classes with all providers and four fresh test forks.

All384 invocations passed with unchanged original provider identities and zero
failures/errors/skips. Flink1.18 median walls were160.522s G1 versus161.618s
Serial; Flink2.2 medians293.982s versus295.568s. Pair differences crossed zero
on both lines. Do not change JDK17 CI collector settings on this evidence.

The [CI guide](../../docs/ci.md#controlled-jdk17-collector-comparison) and
[structured trials](../../docs/benchmarks/ci-profile-20261115.json) retain the
method, all pairs and immutable source/artifact references. This decision does
not change earlier JDK11 tuning or the G1 metadata-dump workaround. Revisit only
with a representative controlled workload showing a consistent material gain.
