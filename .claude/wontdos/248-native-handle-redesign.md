# Defer broader native-handle and wrapper redesign after the bounded boundary investigation

The maintainer requested a timeboxed optimization and merge for
[#248](https://github.com/datafusion-contrib/StreamFusion/issues/248). One bounded
CPU/allocation capture covers physical Calc, aggregate, join and JVM-callback
pipelines with small and large entry batches. One local prototype caches each
compiled Calc's immutable output schema; the existing input-schema cache and
independent Flink operators remain intact. See the
[optimization and complete evidence](../../docs/optimizations/calc-schema-once-c-data.md#cache-calc-output-schemas).

Remaining C Data array wrapping, buffer references and release callbacks are
observable costs, not missing columnar flow or evidence of rowwise transposes
between native operators. The profile does not isolate pure JNI transition
latency. It does not establish that replacing these ownership transfers with a
generic native-handle protocol would improve complete pipelines. No second
profiling pass, buffer pool, hardcoded operator fusion or Flink-runtime
replacement belongs to this bounded task.

Close the investigation with the measured metadata reduction. Retain existing
dictionary imports, JVM callback paths, default admission, batch ownership,
RowKinds/key groups, watermarks, checkpoint boundaries and lifecycle cleanup.
This is not a claim that all boundary overhead has disappeared or that every
native pipeline beats stock Flink. Workload-specific rejected optimizations keep
their own evidence and are not made accelerated by this result.

Reopen only with new evidence identifying a bounded avoidable cost, or a concrete
ownership proposal that preserves independent operators, retained outputs,
dictionaries, exactly-once release, failures/cancellation, memory accounting and
recovery. Compare stock Flink and the current production implementation with
the normal conversions and IPC intact before pursuing a broader design.
