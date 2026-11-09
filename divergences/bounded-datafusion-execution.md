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

Arroyo's ordinary SQL planner rejects non-windowed ORDER BY and LIMIT. Bounded
sorted limits instead follow Comet's `SortExec.with_fetch` followed by
`GlobalLimitExec` for the offset. Flink still owns its local/global physical stages
and singleton exchange; each bounded native task applies only its own bounds.
A logical zero-row limit still consumes input before returning no output, so it
does not suppress upstream evaluation or failures in an existing sort-limit stage.

Comet's plan memory pool waits for spawned-worker reservations to be returned before
releasing its host memory manager. StreamFusion can keep its independent Java owner
registration alive through the last pool reference instead. DataFusion reservations
and worker contexts retain that reference, so asynchronous cancellation cannot
prematurely return their balance to the shared authority. The cached Java authority
class follows Comet's retained JVM-reference pattern.

Unlike a synchronous Calc, the sorter retains inputs across calls. StreamFusion's
Arrow Java authority cannot transfer a borrowed allocation's charge directly into
DataFusion's working reservation. The bounded bridge therefore copies input into
compact native buffers, releases its borrowed Java reference, and retains only one
reusable copy workspace in addition to DataFusion's state. Output is also compacted
under a reservation, then acknowledged after Java import. This costs two copies but
avoids keeping every retained run charged to both owners or exporting tiny slices
that pin an entire native run. View/dictionary representations are not admitted.
Comet's importer supplies the model for releasing unconsumed C Data callbacks on
failure, including a helper in Arrow's package to inspect its C struct lifetime.

Spillable reservations leave space for input and output handoff within the existing
process authority; there is no separate operator cap or unbounded pool. The
[runtime design](../docs/batch-execution-runtime.md) documents this contract and
the blocking Arrow shuffle/replay controls used for SQL admission.
