# Unsupported operators

**Status: Not supported.** These have no native matcher at all — a query containing one falls back
to Flink entirely, per the [all-or-nothing island rule](index.md#the-all-or-nothing-island).

| Operator | SQL surface | Why |
|---|---|---|
| `Correlate` (most shapes) | Lateral table functions; `UNNEST` with a pushed condition the expression engine can't encode, or any condition over a LEFT `UNNEST` | No native path yet. **Exception:** plain INNER or LEFT `UNNEST` of a single `ARRAY`/`MAP`/`MULTISET` column (optionally `WITH ORDINALITY`, INNER including a pushed element filter) **is** native — see [Unnest / Correlate](unnest-correlate.md). |
| `Match` | `MATCH_RECOGNIZE` (CEP / row-pattern matching) | No native path. |
| `GroupWindowAggregate`, `GroupWindowTableAggregate` | The legacy group-window syntax — `GROUP BY TUMBLE(...)`/`HOP(...)`, and proctime group windows | No native path. **Exception:** a legacy event-time `SESSION(...)` group-window routes natively, reusing the [windowed aggregate](window-aggregate.md) operator, when its only window properties are `(window_start, window_end[, rowtime][, proctime])` in that order. |
| `IncrementalGroupAggregate` | The five-node chain a distinct aggregate plans to only when `table.optimizer.distinct-agg.split.enabled` is on (partial local → incremental → final global over a bucket key) | Deliberate non-goal: the knob mitigates state-backend hot-key skew that an in-process distinct set doesn't exhibit. The default mini-batch plan for distinct aggregates — `LocalGroupAggregate` + `GlobalGroupAggregate` with a distinct `MapView` partial — **is** native; see [GROUP BY](group-by.md). |
| `GroupTableAggregate` | `TableAggregateFunction` | No native path. |
| `DropUpdateBefore`, `Values` | Misc | No native path. (A non-temporal `Sort` is parity, not a gap — Flink itself rejects it in streaming.) |
| `LegacyTableSourceScan`, `LegacySink` | Legacy (pre-`DynamicTableSource`/`Sink`) connectors | No native path. |
| `Python*` (`PythonCalc`, `PythonCorrelate`, `PythonGroupAggregate`, `PythonOverAggregate`, …) | PyFlink UDFs | No native path — a Python UDF can't run inside a native island. |
