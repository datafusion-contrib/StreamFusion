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
- [09 — Per-batch watermark assignment](09-per-batch-watermark-assignment.md) — the columnar watermark assigner slices a batch at watermark jumps to replicate Flink's per-row eager emission, and the aggregator drops rows whose window already closed, so deterministic late-data dropping matches byte-for-byte (monotonic batches take a no-slice fast path); the only residual is Flink's own non-deterministic periodic emission.
- [10 — Columnar exchange uses its own hash](10-columnar-exchange-own-hash.md) — the keyed columnar shuffle co-locates each key on a channel with an internal hash, not Flink's key-group hash; safe because the downstream native window re-groups by key in operator state and never uses Flink keyed state.
- [11 — OVER: incremental accumulators, not a window exec](11-over-incremental-vs-window-exec.md) — running `OVER` and window functions (`ROW_NUMBER`) fold a small per-key state in rowtime order (matching Flink) rather than running a DataFusion/Arroyo window plan over buffered rows; investigation showed Arroyo's per-instant `window_fn` is not cumulative, a batch window exec would need unbounded retention, and DataFusion's incremental window evaluator can't be checkpointed — so own-the-state is the only Flink-parity, bounded path.
- [12 — Joins delegate the match, own the state](12-joins-delegate-match-own-state.md) — interval/window joins run the match as a DataFusion `HashJoinExec` (aligned with Arroyo) but own buffering + watermark eviction via Flink state (Arroyo uses its own expiring key-time tables); INNER + equi-key scope.
- [13 — RowKind carriage as a four-way byte column](13-rowkind-carriage-meta-column.md) — a changelog row's `RowKind` rides the Arrow batch as a hidden byte column carrying all four Flink kinds, not Arroyo's two-way `is_retract` flag, so the `-U`/`-D` distinction upsert sinks need survives the boundary (research §6 width gap).
- [14 — Standalone columnar streaming engines (RisingWave, Proton)](14-standalone-streaming-engines.md) — what RisingWave and Proton confirmed (four-way changelog, MIN/MAX value-multiset, degree-free INNER join) and where we differ as a Flink guest (state in Flink not an LSM; updating join probes natively while time-bounded joins delegate to DataFusion; row↔Arrow transpose at host edges).
- [15 — UNNEST as a take-based fan-out, not DataFusion's `UnnestExec`](15-unnest-take-fanout-not-datafusion-unnestexec.md) — we fan rows out with an Arrow `take` (the `expand`/`assign_windows` shape) rather than Arroyo's rewrite-to-`LogicalPlan::Unnest`, because we must match Flink's `Correlate` output and thread `$row_kind$`, and DataFusion's batch kernel is private.
- [16 — Memory accounting reserves the budget up front](16-upfront-managed-memory-reservation.md) — the operator reserves its operator-scope managed-memory fraction once at open (RocksDB's Flink pattern) and the native side enforces it as a bounded DataFusion pool, instead of Comet's per-grow JNI upcall; Flink's `reserveMemory` is binary (no partial grants, no cooperative spill), so per-grow asking buys nothing and would put a JNI crossing on the hot path.
- [17 — DATE_FORMAT/EXTRACT over TIMESTAMP_LTZ: JVM upcall by default, chrono-tz opt-in](17-ltz-datetime-session-zone.md) — a local-zoned timestamp's fields depend on the session time zone, which the native formatter (no tz database) can't reproduce; the default routes the LTZ case through Flink's own zone-aware `DateTimeUtils` via the columnar upcall (byte-parity), with a pure-Rust `chrono-tz` path opt-in behind `allowIncompatible` (the same crate Comet uses; diverges only at tzdb-version/far-future/deep-history/legacy-zone edges).

## Known transitional gap (not yet a deliberate divergence)

We currently substitute native operators individually and transpose Arrow↔RowData
at each operator boundary — which the end-to-end benchmark showed makes a single
substituted operator *slower* than Flink. Comet keeps *full columnar chains* native and
only transposes at the native↔host edges; we adopt the same model — operators tagged
columnar/rowwise, flowing Arrow between columnar ones, transposing only at the boundary —
supplying the columnar record type and transition insertion ourselves since Flink has no
columnar framework. See [08](08-columnar-flow-transitions.md) for the mechanism. It is a tracked
optimization rather than a divergence in *intent*; only the *mechanism* is recorded.
