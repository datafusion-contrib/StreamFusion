# Joins

StreamFusion runs five distinct join shapes natively, each with its own state layout and its own
gap list:

- [Regular join](regular-join.md) — an ordinary equi-join over two changelog inputs.
- [Interval join](interval-join.md) — a time-bounded join (`BETWEEN` on rowtime/proctime).
- [Window join](window-join.md) — both sides carry the same windowing (`TUMBLE`/`HOP`/`CUMULATE`).
- [Temporal table join](temporal-join.md) — `FOR SYSTEM_TIME AS OF` a versioned table (event-time
  only).
- [Lookup join](lookup-join.md) — `FOR SYSTEM_TIME AS OF` a proctime dimension table, driven by the
  connector's own lookup function.

All five require an equi-key of a supported type and a residual non-equi predicate the native
expression engine can express; each page lists its own exact admission conditions.
