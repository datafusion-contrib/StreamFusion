# Event-time OVER: incremental accumulators, not a DataFusion window exec

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo.
**Forced by parity:** partially — the *unbounded-preceding* frame makes the
incremental model strictly necessary for bounded memory; matching Flink's
streaming OVER operator is the prime directive either way.

## Their decision
Arroyo's `window_fn.rs` implements SQL `OVER` (window functions) by **buffering
rows per distinct event-time instant** and running a **DataFusion window
`ExecutionPlan`** over them — full delegation: the operator is a harness that
stages rows into the plan via channels and drains its output on the watermark.
This generalizes to any window function DataFusion supports (`ROW_NUMBER`, `LAG`,
`RANK`, …), because the plan does the work.

## What we do instead — which is what Flink itself does
For the `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` running aggregate, we
keep, per partition key, only the **incremental accumulators** (the same
DataFusion `Accumulator`s the window aggregates use — `sum`/`min`/`max`/`count`,
plus the int-semantics ones in [01](01-integer-truncating-avg.md)). Each row folds
into its key's accumulators in rowtime order and emits the running value; raw rows
are discarded once folded. This mirrors Flink's own `RowTimeUnboundedOver` operator,
which keeps an accumulator per key, not the rows.

## Why not delegate to a DataFusion window exec here
The frame is **unbounded preceding** — every row's result depends on *all* prior
rows of its key, across every batch and watermark. A DataFusion window
`ExecutionPlan` computes over a materialized input, so delegating would require
**retaining every row ever seen, per key, forever** (or re-reading it from state
each emission). The incremental accumulator keeps a single running state per key
instead — bounded memory, no recomputation, and byte-identical to Flink, which is
built the same way. So here the Arroyo-style delegation is not just unnecessary
but strictly worse, and it would also diverge from Flink's actual operator.

We still use DataFusion's compute: the accumulators *are* DataFusion's. What we do
not adopt is Arroyo's *orchestration* (buffer rows, run a plan), because Flink's
orchestration (incremental per-key state) is both the parity target and the
bounded-memory choice. This is the same reasoning as
[03](03-incremental-window-merge.md), applied to OVER.

## Scope, and where delegation *will* be right
We admit only **running aggregates** (`SUM`/`MIN`/`MAX`/`COUNT`/`AVG`) over the
default unbounded frame. `AVG` rides Flink's `$SUM0`+`COUNT` decomposition with the
divide on the host. Bounded/`ROWS` frames and proctime fall back.

General window functions — `ROW_NUMBER`, `RANK`, `LAG`, `LEAD` — are **not**
mergeable accumulators, so when we add them the incremental model does not apply
and the right design is to **delegate to a DataFusion window `ExecutionPlan`, as
Arroyo does** (buffer the frame's rows, run the plan). That will be an alignment,
not a divergence; recorded here so the future choice is deliberate.

## Verification
Parity harness: running `SUM`/`MIN`/`MAX`/`COUNT`/`AVG`, partitioned and not, over
DataStream and Parquet sources, including rowtime ties within the unbounded frame.
