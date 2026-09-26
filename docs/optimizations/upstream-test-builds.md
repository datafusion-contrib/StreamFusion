# Upstream test builds

This optimization reduces CI work and waiting time; it does not change query throughput or
native admission. See the [upstream suite guide](../upstream-flink-suite.md#shared-builds-and-runtime-shards)
for commands and coverage requirements.

Each workflow builds the Flink planner and current StreamFusion payloads once per Flink line,
then transfers those artifacts to the suite jobs. The shared artifact excludes Cargo intermediates
and test evidence. Maven caches the isolated repository actually used by the harness. Connector
jobs compile only their additional fixtures, while each runtime suite is split into four jobs
balanced by historical class durations. Full test selection, parameter counts and native/fallback
execution contracts remain blocking.

Before this change, the September 25 Flink 1.18 inventory job took approximately 101 minutes,
including 85 minutes in the test invocation. The September 26 Flink 2.2 runtime CI job took
approximately 50 minutes, including 31 minutes in the test invocation. These are CI timings from
different runs, not engine benchmarks or a controlled version comparison. A measured end-to-end
improvement must include build preparation, artifact transfer and runner queues; balanced historical
class durations alone are not a measured CI speedup.
