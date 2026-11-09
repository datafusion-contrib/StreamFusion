# Bounded execution runtime groundwork

This internal runtime is groundwork for batch SQL. The SQL planner still admits only
the [documented scalar/UNION scope](operators/batch.md); it does **not** select the
new sort operator. No sort performance opt-in or partial-query substitution is added.

## Execution

One bounded DataFusion execution owns persistent input/output streams for the task.
Each input has one pending Arrow batch, explicit EOF, producer backpressure and
separate reader/driver wakeups. An empty input slot means pending, not completion.
Offering another batch into an occupied slot fails rather than extending a queue.
EOF follows the final offered batch, including an empty or short batch.

The driver returns an output batch, a set of inputs needing data, or completion.
Input demand returns control to Flink; it never waits for another batch that the
same mailbox must deliver. Separate driver wakeups also cover DataFusion operators
that consume inputs on worker tasks. Output is drained incrementally rather than
collected into a result vector or concatenated into one batch.

The JNI sort bridge preserves that protocol. While waiting on asynchronous native
I/O it yields periodically so Java can check task interruption. The deadline cannot
preempt a synchronous CPU-bound poll or synchronous filesystem call. Cancellation
closes inputs and drops the execution stream; DataFusion owns cancellation of any
workers spawned by its operators. A consumed input cannot be replayed: Flink task
retry must construct a fresh execution and replay its source/shuffle input.

The current native sort prototype accepts signed integer keys, explicit ascending/
descending direction and NULL placement, with no LIMIT/OFFSET. Its Arrow input and
output remain columnar. It uses DataFusion's external sorter, the existing shared
Tokio runtime, Flink task spill directories, and the TaskManager off-heap authority.
It does not add a distributed scheduler or change Arrow IPC serialization.

## Memory lifetime

JNI memory callbacks retain the Java authority class as a global reference obtained
on the calling Java thread. Native workers use that reference rather than resolving
an application class through an attached thread's default classloader.

The bounded task registers its own memory owner. Reservations and runtime contexts
retain the pool, and the pool's final drop closes the registration. Java operator
close does not reclaim a balance while a cancelled DataFusion worker still holds
it. Existing streaming operators preserve their Java-owned registration contract.
See the [reference comparison](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/bounded-datafusion-execution.md).

**Admission remains blocked on the memory handoff.** Borrowed input buffers remain
charged through Arrow Java while DataFusion also reserves their buffered footprint.
The prototype therefore has conservative overlapping charges, which can force
earlier spill. Output import can likewise overlap a native reservation until its
producer releases it. Resolve shared-buffer accounting before admitting this path;
do not replace the shared cap with an unbounded pool or silently raise its budget.
Also verify runtime spill metrics, disk/memory exhaustion, isolated-loader execution,
blocking Arrow shuffle/replay and unchanged upstream SQL witnesses before admission.

## Validation and performance

Native controls cover input demand and wakeups, one-slot backpressure, EOF, schema
rejection, prevention of replay, empty results, memory denial, independent
worker-driven inputs and global SUM/COUNT results for empty/all-NULL inputs. A sort
control sorts 131,072 rows under a 256 KiB pool, asserts spill before EOF, bounds
output batches and verifies memory/file release.

JNI operator controls cover multi-key order/NULL placement, EOF, a 262,144-row sort
under a 2 MiB task pool with positive pre-EOF spill files, cleanup after success and
cancellation, creation failure, and task interruption. These are runtime controls,
not upstream SQL admission evidence. Aggregation is currently a native adapter test,
not an exposed batch SQL implementation.

The [operation benchmark](benchmarks/batch-sort-groundwork.md) compares prepared
stock RowData with equal native Arrow data. It does not measure a complete SQL job
and cannot establish an end-to-end speedup. Stock Flink remains the previous and
current StreamFusion SQL route for sort queries.
