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
