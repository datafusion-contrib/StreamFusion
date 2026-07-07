# Synchronous stateful execution on Flink's mailbox

**Kind:** runtime model.
**Diverges from:** Arroyo.
**Forced by parity / correctness:** yes.

## Their decision
Arroyo runs operators as **asynchronous actors** (async Rust tasks driven by a
Tokio runtime), coordinating its own state and checkpoint barriers. Async is
natural there because Arroyo owns the whole runtime and its checkpoint protocol.

## What we do instead
Our stateful native operators run **synchronously on Flink's single mailbox
thread**: `processElement`/`processWatermark` call into native code and block
until it returns. There is no async bridge between the JVM operator and the native
engine for stateful work. State is checkpointed by snapshotting native state into
Flink operator state on Flink's snapshot call.

## Why
Flink's operator contract is single-threaded per task: the mailbox thread owns
the operator, its state, and the alignment of checkpoint barriers with the element
stream. Introducing async execution underneath a Flink operator would break that
contract — a checkpoint barrier could be processed while native work for prior
elements is still in flight, corrupting exactly-once state. Flink itself runs
stateful operators synchronously and reserves async only for stateless
`AsyncWaitOperator`-style I/O. As a guest we adopt the host's model; this mirrors
how Comet executes within Spark's task threads rather than spinning up its own
scheduler.

## Scope / consequences
- Async is still the right tool for the *stateless* cases Flink itself makes
  async: native sources awaiting availability futures, and async I/O / lookup
  joins. Those will use Flink's async patterns, not a bespoke one.
- The guarantee is pinned by the checkpoint-interleaving window-aggregate test
  (buffered input survives a mid-stream snapshot/restore). The one remaining
  async candidate, the async scalar UDF, is tracked — with the within-batch
  rationale for not porting Flink's `AsyncWaitOperator` bridge — in
  https://github.com/datafusion-contrib/StreamFusion/issues/7.
