# Bounded window-rank firing

Arroyo's `crates/arroyo-worker/src/arrow/window_fn.rs` retains execution plans by timestamp and
forwards result streams through its collector on watermark advancement. StreamFusion retains
Flink window Top-N buffers and their released keyed-state encodings instead: canonical interchange,
regional LTZ timer conversion and Flink row-number ties require those existing contracts. Firing
now follows the same bounded-stream structure without substituting a new state representation.

Comet's `native/core/src/execution/utils.rs` exports owned Arrow arrays through the C Data
Interface. StreamFusion reuses its established `bridge::emit_record_batch` synchronous receiver
handoff: the borrowed C structs live through import and imported buffers may outlive the callback.
The JNI guard preserves an original Java callback exception while native output reservations and
FFI wrappers unwind. The operator imports and emits one independently owned Arrow batch per call.

The production limits are 1,024 rows and a conservative 4 MiB workspace target, with task-budget
reservation before decoding and scalar conversion. One wider row is allowed only within the task
budget. RocksDB values retain the existing whole-group framed storage layout but firing copies
and decodes bounded frames, including across many small groups. Per-row ranks are carried across
batch boundaries. Signed window starts require two ordered physical ranges per end/key group;
a numeric comparator on one raw byte-ordered iterator would be incorrect. No state bytes, Java
operator fields, serial UIDs, or canonical identities change.

Focused regressions cover a large single-group N, wide payloads, small-group coalescing, bounded
firing under a workspace budget smaller than the backlog, mixed signed starts within one key group,
malformed frames, receiver exception identity, Arrow cleanup and recovery from the previous native
checkpoint. This is a bounded-memory change; it does not claim a throughput improvement.
