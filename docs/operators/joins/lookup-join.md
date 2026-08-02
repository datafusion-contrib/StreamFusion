# Lookup join

**Status:** Native for INNER and LEFT, both sync and async connectors. `FOR SYSTEM_TIME AS OF
probe.proctime` against a dimension table (Nexmark q13) — each probe row looks the key up in an
external table as of "now," rather than holding versioned state the way the [temporal table
join](temporal-join.md) does. There is no build-side state in the Flink sense at all: the lookup goes
straight to the connector on every probe row (or is cached by the connector itself).

## How it works

The probe batch stays Arrow, but the row-level join core is **Flink's own generated lookup runner** —
key building over both field references *and* constants, the pre-filter, the connector's real
`LookupFunction`/`asyncLookup`, the projection/filter on the temporal table, the residual non-equi
condition, and LEFT null-padding — all driven by the native operator per batch. Because it *is* the
host's generated code invoked per batch rather than a reimplementation, it is byte-identical to Flink
by construction.

The **async** path fires every distinct key in a batch concurrently and awaits before emitting the
batch — the Arroyo/RisingWave within-batch model, needing no operator mailbox since nothing is left
in flight across a batch boundary. Concurrency is bounded by Flink's own
`table.exec.async-lookup.buffer-capacity`. This isn't vectorizable compute — it's a JVM upcall into
the host connector — but it keeps the island unbroken, and the async form overlaps a batch's I/O
instead of serializing it.

## Admission

Same equi-key/type conditions as the [regular join](regular-join.md): a supported-type equi-key and
null-dropping keys (LEFT is the only non-INNER shape here). The temporal table itself must be a
non-legacy `TableSourceTable`. Projection/filter on the temporal table, the pre-filter, the residual
condition, and constant lookup keys are all native — the operator drives Flink's own generated
runner, so none of these narrow admission further.

## Falls back to Flink when

- the planner produces an **upsert-materialized** (keyed-state) lookup rather than a plain
  per-row lookup;
- the join type isn't INNER or LEFT;
- the temporal table is a legacy (pre-`TableSourceTable`) connector.
