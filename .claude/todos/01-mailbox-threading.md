# Non-blocking integration with the task mailbox

**Status:** open
**Source:** research findings §5 (threading model, open question #2)

## Problem
Every native operator today makes its JNI call synchronously on the Flink
task thread, and the window path internally drives a tokio runtime with
`block_on`. Flink's runtime is a single-threaded mailbox: while the task
thread is parked inside native code it cannot process checkpoint barriers,
timers, or watermark mailbox actions. For short, bounded CPU work per batch
this is tolerable (ordinary operators also compute synchronously in
`processElement`), but it becomes incorrect once a native call can block for
a non-trivial time (async I/O, backpressured pulls, large spills).

## Goal
Native operators should never hold the task thread across a long or
potentially-blocking native call. Run the native chain as an autonomous
worker and bridge results back through `MailboxExecutor.execute` for egress,
yielding to the mailbox so barriers/timers still flow. Watch for the
mailbox↔native deadlock the research flagged.

## Acceptance criteria
- A long-running native call does not stall checkpoint barriers (test with a
  deliberately slow native stage + a checkpoint in the harness).
- Backpressure surfaces without busy-waiting.
- Existing operator results unchanged.

## Pointers
- Comet blocks the task thread (`jni_api.rs` `block_on`) — that is the
  pattern we must NOT copy wholesale into a streaming engine.
- Flink `MailboxExecutor`; Arroyo's per-operator async actor + `select!` loop
  (`arroyo-operator/src/operator.rs`) is the reference for the worker side.
