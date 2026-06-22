# Divergences

StreamFusion is meant to be heavily influenced by two existing systems:

- **datafusion-comet** — how to be a *guest accelerator inside a host engine*:
  plug in via a post-optimization rule, cross the boundary with the Arrow C data
  interface over JNI, fall back to the host for anything unsupported, and keep
  the host's plan structure intact so results are identical.
- **Arroyo** — how to do *streaming, stateful, windowed aggregation in Rust over
  Arrow/DataFusion*: the window operators, the accumulator machinery, watermark
  firing, and checkpointing idioms.

The default is to copy their already-made architectural decisions rather than
invent our own. This folder records the cases where we deliberately did *not* —
each file states the influence's decision, what we did instead, and why.

## The root cause of most divergences

Comet and Arroyo play different roles, and where they conflict we resolve it the
Comet way:

> **We are a guest inside Flink. Flink is the control plane and owns the
> semantics; the native engine is only a faster data plane. When Arroyo's
> decision and Flink's behavior disagree, we follow Flink, because byte-for-byte
> parity with the host is the prime directive.**

Arroyo is a *standalone* engine — it owns its SQL semantics, its runtime, and its
checkpoint coordination, so it can choose whatever is simplest or fastest. We
cannot: our output has to equal what stock Flink would have produced for the same
query. So several of the divergences below are "diverge from Arroyo because Flink
requires it," which is really "follow Comet's guest-accelerator principle."

A divergence that is *not* forced by parity (e.g. an internal algorithm choice)
needs a stronger justification, and should still produce identical results to the
host — verified by the parity harness.

## Index

- [01 — Integer-truncating AVG](01-integer-truncating-avg.md) — semantic; match Flink, not DataFusion/Arroyo.
- [02 — Two-phase local/global aggregation](02-two-phase-local-global-aggregation.md) — structural; mirror Flink's plan split, not Arroyo's single stage.
- [03 — Incremental session/sliding merge](03-incremental-window-merge.md) — algorithmic; merge accumulators instead of re-aggregating raw rows like Arroyo.
- [04 — Synchronous stateful execution](04-synchronous-stateful-execution.md) — runtime model; run on Flink's mailbox, not Arroyo's async actors.
- [05 — Cumulative windows](05-cumulative-windows.md) — net-new operator neither influence has; built against Flink's semantics alone.
- [06 — Two-phase slice-sharing](06-two-phase-slice-sharing.md) — hopping's local/global intermediate reverse-engineered from Flink, not Arroyo's raw re-aggregation.
- [07 — Expression encoding + compile-once](07-expression-encoding-and-compile-once.md) — hand-encoded IR following Comet (not Substrait), compiled once per operator.
- [08 — Columnar flow with transitions](08-columnar-flow-transitions.md) — keep columnar by tagging operators columnar/rowwise and transposing only at boundaries (following Comet), not by fusing subtrees; Flink lacks the columnar framework so we supply the record type + transition insertion.
- [09 — Per-batch watermark assignment](09-per-batch-watermark-assignment.md) — the columnar watermark assigner advances the watermark once per Arrow batch (following Arroyo), not per row like Flink; identical results unless a window closes mid-batch on out-of-order data.

## Known transitional gap (not yet a deliberate divergence)

We currently substitute native operators individually and transpose Arrow↔RowData
at each operator boundary — which the end-to-end benchmark showed makes a single
substituted operator *slower* than Flink. Comet keeps *full columnar chains* native and
only transposes at the native↔host edges; we adopt the same model — operators tagged
columnar/rowwise, flowing Arrow between columnar ones, transposing only at the boundary —
supplying the columnar record type and transition insertion ourselves since Flink has no
columnar framework. See [08](08-columnar-flow-transitions.md) for the mechanism and
`.claude/todos/21-native-operator-chaining.md` for the plan. It stays a tracked
optimization rather than a divergence in *intent*; only the *mechanism* is recorded.
