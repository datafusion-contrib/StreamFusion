# Account native memory against Flink's MemoryManager — remaining tail

**Status:** mostly done — every stateful native operator now enforces a managed-memory budget.
This ticket tracks the residual tail.
**Source:** research findings §4 (open question #1)

## What shipped (see divergences/16)
The transformation declares an operator-scope `ManagedMemoryUseCase.OPERATOR` weight
(`streamfusion.memory.operator-weight-mb`, master switch `streamfusion.memory.accounting.enabled`);
the operator reserves the computed fraction from Flink's `MemoryManager` at open and releases at
close (`ManagedMemoryBudget`); the budget crosses JNI once at handle creation and natively becomes a
bounded DataFusion pool + reservation (`OperatorMemory`), resized from incrementally tracked state
bytes (touched entries only — O(batch), not O(state)). Exceeding it throws
`NativeMemoryLimitException` with the remedy in the message; restore is accounted up front, so a
snapshot that no longer fits fails at restore, not as a container OOM. Covered operators: the
aligned-window aggregate family (one- and two-phase), session windows, non-windowed GROUP BY,
changelog normalize, keep-first/keep-last dedup, OVER (unbounded/bounded/window-function),
event-time sort, append-only and retracting Top-N, window Top-N/dedup, and the interval, window,
temporal, and updating joins.

## Remaining
1. **DataFusion-executed fragments' pool wiring.** The interval/window joins run their match as a
   `HashJoinExec` over the buffered batches, and the file sources/decoders run DataFusion scan
   streams — their *transient working memory* still uses an unbounded default `RuntimeEnv`. Wire the
   operator's bounded pool into those `SessionContext`s (comet's `RuntimeEnvBuilder::with_memory_pool`)
   so the join build side and scan buffers are accounted too.
2. **Local (mini-batch) GROUP BY pre-aggregate.** Its state drains at the mini-batch marker /
   pre-checkpoint flush, so it is interval-bounded, but a high-cardinality interval can still spike;
   same pattern applies if it proves worth the signature churn.
3. The **Java-side Arrow allocator** (`NativeAllocator.SHARED`) is transient per-batch and
   refcounted, deliberately outside the budget (accounting is the pool's job, not the allocator's —
   see the class comment). Revisit only if transpose buffering ever becomes long-lived.
4. **Spill gap** stands: a denied reservation is a hard failure because streaming operators have no
   runtime spill; the structural answer is disaggregated/cached state (ticket 37).

## Pointers
- `OperatorMemory` / `ManagedMemoryBudget` / `NativeManagedMemory`, divergences/16.
- Comet `native/core/src/execution/jni_api.rs` (`prepare_datafusion_session_context`) for the
  RuntimeEnv wiring of DataFusion-executed fragments.
