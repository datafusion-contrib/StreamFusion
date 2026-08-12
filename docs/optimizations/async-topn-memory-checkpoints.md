# Asynchronous Top-N memory checkpoints

**Applies to:** append-only Top-N on the memory backend

Flink's heap backend captures a copy-on-write state-table view at an aligned checkpoint barrier and
serializes that view on its asynchronous checkpoint thread. Native Top-N previously encoded every
key-group IPC payload inside `snapshotState`, so the task could not resume until the complete state
had crossed Rust → Java and been installed in canonical keyed state.

The memory path now captures an immutable native snapshot token at the barrier. Payload rows retain
their existing `Arc<OwnedRow>` buffers; the capture copies the partition keys, small sort keys, and
buffer vectors needed to isolate the checkpoint from subsequent input. Each immutable partition is
then installed as a lazy Flink heap-state value. Flink's normal asynchronous heap-state serializer
calls back into Rust to build the IPC payload. The token is released deterministically after its last
partition is encoded, with a `Cleaner` fallback for cancelled checkpoints.

The state contract remains backend-independent and rescalable: there is one checksummed value per
owned key group, canonical savepoints use the same serializer, and restore accepts both the new v2
lazy descriptor and the previous v1 chunked-byte descriptor. Retracting and update-fast Top-N keep
the v1 synchronous path until they receive their own immutable capture shape.

## Measurement

On the release+mimalloc 2M-event q19 exactly-once Kafka loop (parallelism 4, one-second aligned
checkpoints), a matched 35-second CPU profile moved all 401 native partition-encoding samples under
Flink's `AsyncOperations` thread. Task-thread checkpoint work fell from 506/18,177 samples (2.8%) to
213/17,288 (1.2%), a **56% reduction in checkpoint CPU blocking the task**. Both 75-second loops
completed 9 jobs; median execution time moved from 7.7 s to 7.5 s, so no headline throughput gain is
claimed. The technique is retained for the measured barrier-critical-path reduction, not presented
as a larger end-to-end Q19 speedup.

The diagnostic property `streamfusion.state.asyncMemorySnapshots.enabled=false` selects the legacy
synchronous path for matched profiling; production defaults to the asynchronous path.
