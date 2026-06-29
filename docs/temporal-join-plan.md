# Native temporal join — implementation plan

Adds a native rowtime **temporal table join** (`... JOIN versioned FOR SYSTEM_TIME AS OF o.rowtime`)
as a columnar two-input operator, closing one of the largest "Flink does it, we don't" gaps. Lookup
join is deliberately **not** ported (see the end of this doc and `coverage-and-fallbacks.md`).

## Why temporal, not lookup

Both were scoped together; the research (Flink `TemporalRowTimeJoinOperator` /
`AsyncLookupJoinRunner`, RisingWave `temporal_join.rs` / `lookup.rs`, Arroyo `lookup_join.rs`)
settles it:

- **Temporal join needs no mailbox.** Flink's `TemporalRowTimeJoinOperator` /
  `TemporalProcessTimeJoinOperator` are plain synchronous `TwoInputStreamOperator`s: keyed state +
  event-time timers + watermarks, no `MailboxExecutor`/async. The build side is *internal* versioned
  state. This is the same shape as our window/interval join — a natural columnar-island fit.
- **Lookup join is I/O, not compute.** Its payload is an external call into a host-Java
  `LookupFunction` (JDBC/HBase/…); the async path needs `AsyncWaitOperator` + the mailbox purely for
  ordered emit. There is nothing vectorizable, and running it inside the all-or-nothing island would
  mean JNI-upcalling the JVM connector per batch. Kept as an intentional, documented fallback. Arroyo
  only makes lookup native because *its connectors are native Rust*; Flink's are not.

## Scope (this pass)

- **Rowtime temporal table join only.** `rightTimeAttributeIndex >= 0`, `!isTemporalFunctionJoin`.
- Proctime temporal *table* join: Flink throws `"Processing-time temporal join is not supported
  yet."` (FLINK-19830). Legacy temporal *function* join (`LATERAL TemporalTableFunction(proctime)`)
  is INNER-only deprecated API. Both fall back — matching Flink is correct here, not a gap.
- Join types: **INNER and LEFT** only (Flink rejects RIGHT/FULL for temporal join). The right side is
  never the outer side, so only left-outer null-padding is possible.

## Semantics to replicate (deterministic — must match Flink, value-compared)

The operator is **keyed by the equi-join key**. Per key (a faithful port of
`TemporalRowTimeJoinOperator.emitResultAndCleanUpState`):

- **Right (versioned) state:** `rightTime -> (row, RowKind)`, last-write-wins per `rightTime`, all
  RowKinds stored. Sorted by `rightTime`.
- **Left (probe) state:** rows buffered in arrival order, each with its `leftTime`.
- **On watermark advance:** for every buffered left row with `leftTime <= watermark`, in arrival
  order: binary-search the right versions for the latest `rightTime <= leftTime`; if found **and it
  is an accumulate message** (`+I`/`+U`) **and the residual predicate holds**, emit `[left.., right..]`
  carrying the left row's RowKind; otherwise, for a LEFT join, emit `[left.., null..]`. Remove the
  processed left rows.
- **Right cleanup:** keep the latest version `<= watermark` and everything newer; drop older versions
  (Flink's `firstIndexToKeep`).

Because emission is gated on the watermark, the result *multiset* is independent of arrival
interleaving and of cross-key emission order — so `NativeParity.assertParity` (which sorts) is a valid
exact check. A changelog left input uses `assertChangelogParity`.

## Layers (mirrors the interval/window join wiring)

1. **Rust `TemporalJoiner`** (`native/src/lib.rs`), row-oriented over the existing
   `GroupKey`/`JoinRow`/`ScalarValue` primitives (same as `UpdatingJoiner`):
   `left_state: HashMap<GroupKey, Vec<LeftEntry>>`, `right_state: HashMap<GroupKey, BTreeMap<i64,
   (JoinRow, i8)>>`. Methods `push_left`/`push_right` (buffer, no output), `advance(watermark)` (emit
   + cleanup), `snapshot`/`restore`. Residual predicate via the shared `JoinPredicate`. Output batch
   = `[left.., right..]` + `$row_kind$`, same builder as `UpdatingJoiner::push`.
2. **6 JNI entry points** `create/pushLeft/pushRight/advance/close/snapshot/restore TemporalJoiner`
   (reuses `read_join_predicate`, `import_record_batch`, `export_record_batch`).
3. **`Native.java`** native-method decls.
4. **`NativeTemporalJoinOperator`** — `TwoInputStreamOperator` (no `ProcessingTimeCallback`; event
   time only). `processElement1/2` buffer; `processWatermark` advances + emits; `finish` drains via
   `advance(Long.MAX_VALUE)`; checkpointed handle.
5. **`TemporalJoinMatcher`** — extracts equi keys from `joinSpec()`, and `leftTime`/`rightTime` by
   walking the join condition for the `__TEMPORAL_JOIN_CONDITION` RexCall (matched by operator name to
   avoid Scala interop); residual = condition with that call stripped. Rejects proctime / temporal
   function / non-INNER-LEFT / unsupported key types.
6. **`StreamPhysicalNativeTemporalJoin`** (`requireWatermark()=true`) + **`NativeTemporalJoinExecNode`**.
7. **`PhysicalPlanScan`** branch on `StreamPhysicalTemporalJoin`, gated `operatorEnabled("temporalJoin")`,
   `columnarInput` on both sides by their keys; `unsupportedReason` dispatch for fallback visibility.
8. **Tests:** operator harness (`+I/+U/-D` on input 2, left buffering, watermark emission, version
   selection, left-outer null-pad, residual predicate) + `FlinkTemporalJoinSqlHarnessTest` (versioned
   table via `ROW_NUMBER() … ORDER BY rt DESC` dedup view) parity.
9. **Docs:** `coverage-and-fallbacks.md` (temporal join → native; lookup join → intentional fallback)
   + readme compatibility chart, same commit.
