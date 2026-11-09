# Bounded DataFusion task execution

The implementation was designed after consulting Comet's execution JNI driver,
scan and plan memory pool, and Arroyo's stateless executor, buffered join execution,
incremental aggregate and cloneable stream futures.

It follows their shared structure: compile a DataFusion physical plan, supply Arrow
input through a pending/EOF scan, retain its task context, and poll its output stream.
It does not introduce another distributed runtime or collect an entire result.

Flink delivers input through a push/mailbox operator, unlike Spark's pull iterator.
The adapter therefore reports input demand to the host instead of recursively
calling Java for another batch. A one-slot port provides backpressure, and a
separate driver waker handles scans polled by DataFusion workers. The JNI driver
also yields during pending native I/O so the task thread can check interruption.

Arroyo resets bounded physical execution for a streaming batch/window. Here a
bounded Flink task executes its plan once across all incoming batches, with one
explicit EOF per input. Task retry rebuilds that execution; inputs are not rewindable.

Comet's plan memory pool waits for spawned-worker reservations to be returned before
releasing its host memory manager. StreamFusion can keep its independent Java owner
registration alive through the last pool reference instead. DataFusion reservations
and worker contexts retain that reference, so asynchronous cancellation cannot
prematurely return their balance to the shared authority. The cached Java authority
class follows Comet's retained JVM-reference pattern.

Shared Arrow/DataFusion buffer accounting is still unresolved for planner admission;
the internal sort prototype currently over-reserves borrowed input buffers. This
does not authorize a different memory budget, an unbounded runtime, or default SQL
sort admission. The remaining acceptance work is tracked with the active local
batch implementation goal and described in the [runtime design](../docs/batch-execution-runtime.md).
