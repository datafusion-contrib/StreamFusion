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

The Paimon 1.0 compatibility workflow builds release native libraries during source/sink parity
and reuses them for the upstream SQL suite in the same job. Previously it compiled both debug
and release libraries: the [baseline run](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36557001611)
took 110.5 minutes, including approximately 31 minutes of debug compilation and 49 minutes of
release compilation. The upstream phase now uses two JVMs with distinct fork identities and
separate evidence directories; tests remain serial within each JVM. See the
[Paimon guide](../connectors/paimon.md) for configuration and reuse prerequisites. These changes
remove duplicate compilation and allow independent classes to overlap; the hosted post-change
speedup has not yet been measured.
