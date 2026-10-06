# Retire dead Delta checkpoint buffers

**Applies to:** native Delta upsert and delete buffering before a checkpoint

A checkpoint interval can contain many replacements of a small primary-key domain. The writer
previously marked superseded positions dead but held every input Arrow batch and its selection
metadata until the checkpoint. Its retained payload therefore grew with arrivals even when the
final live keys stayed constant.

Each selection now counts live positions. Removing its last live row immediately releases its
Arrow reference and removes the selection from an insertion-ordered set; an empty partition
buffer also leaves the partition map. Adding a replacement precedes removing its incumbent, so
same-batch replacements keep the Arrow owner alive. A partially live batch retains its existing
checkpoint lifetime. Append-only rows still need every input batch and gain no retention saving.

Primitive growable integer arrays hold row ordinals, and a bitset tracks live positions. Draining
visits set bits instead of boxing positions or scanning dead flags. Existing partition lookup
uses a map get; partition creation happens only on a miss. Metadata removal is constant time and
preserves live selection order. Checkpoint output and the published Delta merge/commit APIs are
unchanged. Arrow ownership follows the explicit retain/release pattern consulted in Comet's
Arrow importer.

## Measurement

`DeltaBufferingBenchmark` times production writer arrivals over a held checkpoint. Setup builds
input outside timing; commit and released Delta Kernel readback verify final values afterward.
It compares append with upserts over 16 keys, 1,024-row input batches, 4,096/32,768 rows, and
16/256-byte payloads. The initial validated wide 32,768-row upsert comparison reduced retained
input Arrow bytes from **10,551,296 to 329,728 (32×)**. This measures retained input ownership,
not copied bytes or all task memory. Partially live buffers can still retain dead wide payloads.

The first eager-retirement implementation regressed the wide arrival median by about 12%
and added heap allocation, so it was rejected as the final implementation. Primitive positions,
bitset liveness, and lookup without a per-row creation callback were added before the final run.

The final paired release run used identical fixtures at baseline `96f98a32` and the candidate,
seven observations per shape after one warmup. All five lifecycle tests and existing Delta parity
tests passed. All eight arrival medians improved, but separate JVMs, fixed shape order, and short
warmup leave substantial JIT/order variation. The large wide upsert improved only **1.09×**, with
an overlapping range and a larger candidate IQR; do not present the largest small-case ratio as a
general throughput gain. Raw observations and inclusive quartile statistics are in the
[portable results JSON](../benchmarks/results/delta-live-buffer-retirement.json).

| Input rows | Payload bytes | Mode | Before median ms | After median ms | Median ratio | Before range / IQR ms | After range / IQR ms |
| ---: | ---: | --- | ---: | ---: | ---: | --- | --- |
| 4,096 | 16 | append | 1.234 | 0.868 | 1.42× | 0.905–8.485 / 3.612 | 0.784–2.746 / 1.587 |
| 4,096 | 16 | upsert | 2.031 | 1.244 | 1.63× | 1.472–16.320 / 1.139 | 0.957–2.646 / 0.696 |
| 4,096 | 256 | append | 1.916 | 0.504 | 3.81× | 1.772–3.013 / 0.591 | 0.474–0.552 / 0.055 |
| 4,096 | 256 | upsert | 1.723 | 0.833 | 2.07× | 1.110–5.122 / 1.468 | 0.773–1.152 / 0.038 |
| 32,768 | 16 | append | 4.999 | 3.834 | 1.30× | 4.409–7.208 / 2.185 | 3.515–8.408 / 3.613 |
| 32,768 | 16 | upsert | 9.192 | 6.742 | 1.36× | 6.671–19.231 / 2.747 | 5.996–9.797 / 0.763 |
| 32,768 | 256 | append | 4.119 | 3.655 | 1.13× | 3.840–11.452 / 1.676 | 3.520–13.947 / 0.787 |
| 32,768 | 256 | upsert | 6.933 | 6.376 | 1.09× | 5.818–22.943 / 3.759 | 6.062–26.506 / 8.578 |

| Input rows | Payload bytes | Mode | Median JVM allocated bytes, before → after | Retained input Arrow bytes, before → after | Input Arrow requests, both |
| ---: | ---: | --- | ---: | ---: | ---: |
| 4,096 | 16 | append | 2,176,880 → 1,968,432 | 393,216 → 393,216 | 16 |
| 4,096 | 16 | upsert | 3,390,032 → 3,181,680 | 401,408 → 100,352 | 20 |
| 4,096 | 256 | append | 2,406,256 → 2,066,736 | 1,310,720 → 1,310,720 | 28 |
| 4,096 | 256 | upsert | 3,390,032 → 3,181,680 | 1,318,912 → 329,728 | 32 |
| 32,768 | 16 | append | 18,199,680 → 16,531,728 | 3,145,728 → 3,145,728 | 128 |
| 32,768 | 16 | upsert | 27,113,312 → 25,445,936 | 3,211,264 → 100,352 | 160 |
| 32,768 | 256 | append | 18,199,680 → 16,531,728 | 10,485,760 → 10,485,760 | 224 |
| 32,768 | 256 | upsert | 27,113,312 → 25,445,936 | 10,551,296 → 329,728 | 256 |

Heap allocation covers the entire writer bookkeeping boundary, not only selection metadata.
Upsert retention falls 4× for 4,096 rows and 32× for 32,768 rows; append retention is unchanged.
Input Arrow allocation requests depend on width and RowKind sidecar as shown above, unchanged
before/after, and every write-phase Arrow allocation-request observation is zero. Neither metric proves bytes copied.
The final large wide upsert heap median falls **27,113,312→25,445,936 bytes** (about 6.15%).

Fixture cleanup retries released checksum-writer races outside timing, using identical
before/after code. This arrival-only improvement is not an end-to-end stock/native speedup.
The existing released-only full Delta sink comparison remains separate; see the
[Delta connector](../connectors/delta.md) for its pipeline harness and results.
