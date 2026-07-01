# Native lookup join — extensions beyond the v1 sync path

**Status:** v1 DONE (2026-07-01). This ticket tracks the follow-ups the v1 operator deliberately gates
out (they fall back to Flink today).

## What v1 does

`NativeLookupJoinOperator` accelerates a **synchronous** processing-time lookup join (Nexmark q13):
`probe JOIN dim FOR SYSTEM_TIME AS OF probe.proctime ON probe.k = dim.key`. It stays an Arrow
operator so the probe-side Calc/source keep running natively (the whole point — otherwise the lookup
would break the columnar island and drag the surrounding operators back to rowwise), and per probe row
it calls the connector's real `LookupFunction` (obtained via `LookupJoinUtil.getLookupFunction`, exactly
as Flink's `LookupJoinRunner` does), so the result is byte-identical. INNER + LEFT, field-reference
lookup keys, no calc/residual. Wiring: `LookupJoinMatcher` → `StreamPhysicalNativeLookupJoin` →
`NativeLookupJoinExecNode` → operator; verified by `FlinkLookupJoinSqlHarnessTest`.

## Follow-ups (each currently falls back with a named reason)

- **Async lookups + the mailbox.** An `AsyncLookupFunctionProvider` connector (`isAsyncEnabled`) needs
  the operator **mailbox** (`MailboxExecutor`): fire `asyncLookup` returning a `CompletableFuture`, and
  enqueue result-handling mails to emit on the task thread in order (Flink's `AsyncWaitOperator` /
  `TableKeyedAsyncWaitOperator` pattern). This is where the "you may need the mailbox" note lands — v1
  is sync so it doesn't; async does. Keep ordering + watermark semantics.
- **Calc on the temporal table.** `calcOnTemporalTable` (projection/filter on dim rows before the join)
  — evaluate it natively via the existing expression engine over the dim columns, or in the JVM bridge.
- **Residual (non-equi) join condition.** `finalRemainingCondition` / `finalPreFilterCondition` —
  evaluate over the joined batch with the native Calc engine (the pre-filter gates the probe row before
  the lookup, as Flink does).
- **Constant lookup keys.** v1 admits only `FieldRef` keys; support `FunctionCallUtil.Constant` (a
  literal key component) by materializing the constant into the key row.
- **Columnar assembly / preload for bounded dims.** v1 copies probe+dim rows per output row. For a
  bounded side input, preload the whole dim into a native hash table once (RisingWave/Arroyo cache the
  dim side) and probe fully columnar — zero per-batch JVM crossing in steady state. Otherwise keep the
  per-row `LookupFunction` call but assemble the output columnar (take probe cols by index + matched dim
  cols) instead of row copies.
- **Distributed execution.** Like the UDF bridge (ticket 38), the operator holds a `LookupFunction`
  built at plan time in the planner JVM — fine for local/embedded/benchmark. For distributed task
  managers, carry the serializable `TemporalTableSourceSpec` into the operator and build the function at
  `open()` (what Flink's own exec node does).
