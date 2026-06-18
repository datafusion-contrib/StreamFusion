# Non-blocking integration with the task mailbox

**Status:** designed — build the async bridge (see subtleties before coding)
**Source:** research findings §5 (threading model, open question #2)

## Where we are
Today every native call runs synchronously on the Flink task (mailbox) thread.
For the current operators that is bounded per-batch CPU work that returns
promptly, so checkpoint barriers and timers already interleave correctly (the
checkpoint/restore test demonstrates this). The risk the research flags —
holding the mailbox thread — only bites once a native call can block for a
non-trivial time (async I/O sources, backpressured native pulls, large spills).

## Goal
Run native work off the mailbox thread and keep the mailbox responsive, so a
long native call cannot stall barriers/timers, **without** changing results.

## Design
Per operator: a single-thread FIFO executor runs native calls (FIFO preserves
operation order). The task thread submits a call and waits by yielding the
mailbox until the result is ready, then emits on the task thread.

## Subtleties found while starting to build it (these shape the work)

1. **Mailbox plumbing.** An operator only gets a `MailboxExecutor` if its
   operator *factory* implements `YieldingOperatorFactory`. We currently wrap
   operators in `SimpleOperatorFactory` (via `ExecNodeUtil`), which does not
   provide one — so this needs a custom yielding factory.

2. **Checkpoint atomicity (correctness-critical).** Yielding the mailbox
   mid-call means a checkpoint barrier can be processed *while* native state is
   being mutated, snapshotting inconsistent state. `snapshotState` must drain
   any in-flight native call (await the future) before snapshotting. Reentrancy
   to reason through: processElement → await → yield → snapshotState → drain the
   same in-flight future.

3. **Verifiability.** The operator test harness is single-threaded/synchronous,
   so the dangerous "checkpoint arrives mid-native-call" race is not
   reproducible without a **slow-op test hook** (a latch-blocked native call the
   test controls). We need that hook to *verify* the bridge upholds identical
   results + consistent checkpoints, which the project requires of every change.

## Verification plan
- Existing parity + checkpoint/restore tests confirm functional identity.
- A latch-based slow native op to deterministically force checkpoint-during-
  in-flight and assert the snapshot/restore stays consistent.

## Pointers
- Flink `AsyncWaitOperator` / `YieldingOperatorFactory` / `MailboxExecutor`.
- Arroyo's per-operator async actor + `select!` loop is the worker-side model.
