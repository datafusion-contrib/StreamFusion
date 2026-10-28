# Batched native format decode

**Applies to:** every byte-emitting native source's decode path (Kafka, CDC, file formats)

## What it is

Raw message bytes decode straight to a typed Arrow batch in one native call per batch, replacing
the host's per-record byte→tree→row materialization — the dominant per-record cost on the hottest
edge (`1089ef0`, `a5e36e7`).

## Why it works

The shallow path keeps the connector's JVM consumer intact — offsets, auth, checkpoints all stay in
Java — and accelerates only the decode itself. Because it only replaces the decode step, it works
for any byte-emitting source, not just Kafka.

## Keyed Kafka tables

A keyed table's keys and values each decode in one call per batch, and the rows pair up per
record through the record index every decoder reports, instead of one value decode per record. On
ten million raw-keyed JSON records the native run went from 3.409 s to 1.877 s (1.82×, or 1.60× to
1.88× against each run's own stock and previous routes), from trailing Flink's decode to leading
it by 1.46×
([archive](../benchmarks/kafka-keys-20261027/README.md#raw-keys-before-batching)).
