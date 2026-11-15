# Validate complete event-time OVER imports before mutation

Arroyo's `arrow/window_fn.rs` keeps time-partitioned pending work separate from execution and
advances it with watermarks. StreamFusion retains that structure. Stock canonical interchange
additionally has to preserve Flink's separate pending-row map, raw accumulator and cleanup/timer
protocol; those cannot be reconstructed from emitted results.

Read-only preparation validates the same inputs used by mutation, using temporary projected
keys rather than seeding the operator's key codec or schema. A later invalid chunk can therefore
fail before any pending row or deadline is applied. Comet's moved Arrow C Data ownership remains
the handoff model: both successful and failed validation consume their imported handles and
release temporary arrays, without retaining them as state. Export still visits live state; this
read-only preparation adds no native snapshot intermediate or checkpoint conversion. The callback
ownership extension below additionally preserves native recovery metadata.

This is groundwork for the released event-time stock adapter, not complete OVER qualification.
The operator page and #569 track the remaining timer/planner/full-job work. Benchmarking is
explicitly deferred.

Imported Arrow ownership must end before a Java exception is installed. JNI validation therefore
finishes its local imported-batch scope before reporting failure, matching the existing fold
validator. Releasing Java-owned buffers with a pending exception can swallow the failure; the
malformed-input controls exercise this ordering as well as target-state immutability.


Actual processing-time callbacks are needed to match released Flink's event-time cleanup during
idle input periods. Arroyo's watermark-driven pending-work structure is retained; the callback
contract additionally preserves Flink's cleanup VALUE independently of whether its timer remains
armed. Java signed-overflow arithmetic can consume a timer without scheduling another. Ordinary
native checkpoints therefore preserve an optional unarmed marker rather than inventing a timer
on restore. Existing eight-byte store stamps and cleanup batches without that marker remain armed.
The marker is native recovery/internal transfer metadata and must be projected into stock timer
queues by the eventual stock adapter, never written as a StreamFusion stock-state descriptor.
