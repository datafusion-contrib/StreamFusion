# Borrow bytes, copy only on first insert

**Applies to:** the updating join's two state levels, the keyed exchange's key-group assignment,
the GROUP BY aggregate's state map, changelog normalize, the keyed `OVER` loops, the retracting
Top-N, keep-first dedup, keep-last dedup, and the append-only Top-N's partition map

Every keyed native operator follows the same discipline: state maps key by `ByteKey` (`Box<[u8]>`
with `Borrow<[u8]>`), so the per-row probe hashes the *borrowed* encoded bytes straight out of the
batch's `Rows` block, and a key already in the map allocates nothing. Bytes are copied exactly once
— when a key, group, or partition first appears — not on every row that happens to match it. This
is the same borrowing discipline described on the [arrow-row byte state](arrow-row-state.md) page,
applied specifically to the *probe* side of every keyed map.

## Updating-join state probes borrow their bytes

Both sides' state maps key by raw arrow-row bytes (`ByteKey`, `Borrow<[u8]>`), so the per-row probe
hashes the borrowed encoded key/row and allocates only when a key or distinct row first enters
state — previously every input row paid two `OwnedRow` heap copies whether or not it was already
stored, the system-allocator signal the differential profile flagged vs. Flink's pooled
`BinaryRowData`. Emit/snapshot reconstruct rows from stored bytes via the converter's parser (wire
format unchanged).

q20 gained +4% on the generator loop. The Proton-style block store (state as columnar blocks + row
refs, emit by `take`) was rejected on that post-round profile — the stored-row decode no longer
registers.

## Flink BinaryRow keys encode per batch, probe borrowed

The operators that must key by Flink's `BinaryRowData` bytes rather than arrow-row — the keyed
exchange's key-group assignment, the GROUP BY aggregate's state map, changelog normalize — encoded
per row: each row re-walked the key type schema, allocated a fresh writer buffer, re-downcast every
key column, and copied the result into an owned map key even when the key was already in state. A
2026-07-12 profile put the path at ~12% of q17's whole job, almost all of it allocator traffic
rather than encoding.

A per-batch encoder now sets up the schema walk, column handles, and row buffer once, writes each
row into the reused buffer, and the group-aggregate/normalizer probes borrow the bytes, owning a
key only on first insert — the same discipline the updating join already had.

Measured with the transpose string fix on the 2M-event generator rung, every keyed query gained:

- q20 **+28%**
- q16 **+25%**
- q9 **+22%**
- q17 **+21%**
- q18 **+16%**
- q23 **+14%**
- q4 **+13%**
- q11 **+12%** native throughput

q16 went 1.03x → 1.27x vs. Flink, q9 went 1.07x → 1.32x, q20 went 0.82x → 0.99x. The Kafka
full-native rung's keyed cells moved +4 to +24 points.

## The ScalarValue-vintage keyed loops retired

The last operators still building a `Vec<ScalarValue>` key (or whole row) per input row moved to
the same arrow-row byte state as the rest: all three keyed `OVER` loops (running fold, bounded-frame
buffers, `ROW_NUMBER`/`RANK` counters) probe by borrowed key bytes; the **retracting Top-N** adopted
the append-only ranker's whole structure — memcomparable sort-key bytes replace the scalar
comparator, `Arc`-shared payload rows make the per-row before/after top-N snapshots refcount bumps
instead of row deep-clones, and the shared distinct-row emit decode applies; keep-first dedup's
emitted-key set probes borrowed bytes; and the exchange split hashes each row's encoded key bytes
from one vectorized pass.

Criterion (4096-row batches, 64–256 keys):

- OVER running sum: 422 → 183 µs (**+121%** throughput)
- `ROW_NUMBER`: 342 → 131 µs (**+162%**)
- bounded frame: 688 → 452 µs (**+52%**)
- retracting Top-N: 10.2 → 3.1 ms (**+228%**)
- exchange split: 174 → 57 µs (**+208%**)
- keep-first probe: **+6%**

The exchange's concrete key→channel assignment changed with the hashed representation — permitted
because co-location, not a specific key→channel mapping, is the exchange's only real contract.

Still scalar-keyed as of this writing, pending a bench that says they matter: the window Top-N
ranker and the temporal join — tracked in the
[perf backlog](https://github.com/datafusion-contrib/StreamFusion/issues/14).

## Deep dive: steady-state probes borrow, only first inserts copy

State maps key by `ByteKey` (`Box<[u8]>` with `Borrow<[u8]>`), so the per-row probe hashes the
*borrowed* encoded bytes straight out of the batch's `Rows` block and a key already in the map
allocates nothing — the bytes are copied exactly once, when a key/group/partition first appears.

Shipped for the updating join's two state levels first (q20 +4%, q23 +21% cumulative in the
2026-07-04 round), then extended to:

- the changelog GROUP BY's group map,
- the keep-last deduplicator, whose stored payload became `Arc<[u8]>` — the replacing row is copied
  once into state, and the `-U` *moves* the replaced payload out, so an ignored stale row now
  allocates nothing at all,
- the append-only Top-N's partition map, where a row dropped at rank > N allocates nothing.

The GROUP BY's emitted changelog keys are borrowed slices too, decoded once per batch.

The second stage measured q23 **+8.5%**, q18 **+5.4%**, q16 **+3.4%** on the generator profile
loop — and its q23 profile is what closed the block-store question: stored-row decode no longer
registers in the joiner at all.


## Experimental JVM aggregate keys

The default-off [JVM aggregate investigation](../operators/group-by.md#jvm-aggregate-investigation)
extends the same probe-before-owning discipline to bounded INT/BIGINT and STRING accumulator caches.
The input row's grouping field probes a primitive numeric map or a STRING hash index. A hit reuses the immutable
Flink BinaryRow key already owned by the entry; a miss uses the released key selector, whose
result is an owned copy. NULL has a separate entry. Optimized input-field indices are retained
explicitly and shown in the plan. Flink managed state, DataViews and checkpoint ownership are
unchanged; snapshot boundaries invalidate cached references.

The allocation profile motivated this change: constructing keys accounted for about 18% of
sampled allocations under the high-cardinality JVM aggregate. Matching hot-key profiles then
sampled 849 MB versus 25 MB through the key selector, and 3.63 GB versus 2.74 GB in total under
the operator, over 5M native input rows per profile. The baseline records 3.28 external CPU
seconds and the candidate none. These samples describe Java allocation estimates, not copied
bytes, Rust heap totals or peak memory; profiled timings are not speed claims.

On released Flink2.2.1/JDK17, 2M rows/65,536 keys, p=1, release/mimalloc, production IPC,
2 GiB heap, row source/blackhole sink and both transposes, the native median moved from
0.703 s before key reuse to 0.672 s after. Corresponding paired stock medians moved from
1.046 s to 1.086 s, so separate-JVM drift limits attribution. The native ranges were below
stock in both runs. At 16 keys the candidate measured 0.505 s versus stock 0.576 s.
Every job owns its metrics cluster; timing includes planning/submission, functions, state
and perimeter conversions, with two warmups and five alternating measured pairs.

This remains an investigation: the clean 2M-distinct-key predecessor measured 2.165 s native
versus stock 1.767 s, and the candidate's distinct-key repeat observed an external build.
The feature and cache stay default off while that regression and remaining contracts are
resolved. [Full trials and mixed Java/Rust profiles](../benchmarks/results/jvm-aggregates-investigation-2026-10-15.json)
retain unfavorable and contaminated measurements as well as the wins.


When a bounded numeric cache fills without reuse and another key arrives, staged state flushes
before subsequent accesses delegate directly to Flink. This avoids repeatedly staging distinct
keys while retaining the cache when its working set fits. The initial cold-key bypass comparison
(1.770 s native / 1.769 s stock) observed external work and remains diagnostic; clean comparisons
are pending. [Isolated diagnostics](../benchmarks/jvm-aggregates.md) retain build/test separation,
all typed key/cardinality trials and mixed Java/Rust profiles without asserting admission.


Isolated runs on both released Flink lines now confirm medium numeric gains and STRING
regressions for the prior uncached STRING path. Medium STRING key-selector stacks sample
1.87 GB on 2.2 and 1.81 GB on 1.18 over 5M native rows per profile. The subsequent STRING
experiment reuses owned BinaryRow keys on hits and retains StringData views only over those
owned keys. The copying-probe predecessor still regresses, with a medium-key lookup hotspot.
The current probe borrows a mutable binary-string view over Arrow UTF-8 bytes only during
ingest, following Comet's borrowed UTF-8 view discipline; it never becomes a retained key.
One Flink MemorySegment wraps the current data buffer per batch, and offsets reposition the
probe without allocating a byte array/string wrapper per lookup. NULL and empty buffers keep
the released binary-string equality/hash contract. No borrowed Arrow string survives batch close. Its entry selection also feeds the accumulator
read, avoiding a second lookup/string view on hits. NULL and cold bypass retain the existing
contracts. The borrowed probe passes all 49 combined semantic/recovery/rescale/ownership cases on both
Flink lines at both capacities. Its first whole-job repeat observed external work; throughput
admission remains unproven.
[Isolated baseline trials and profiles](../benchmarks/results/jvm-aggregates-isolated-2026-10-16.json)
retain unfavorable results and broad distinct-key ranges. Both experiments remain default off.


[The copying-probe follow-up](../benchmarks/results/jvm-aggregate-copying-string-probe-2026-10-16.json)
retains three clean complete-job comparisons and its Java/Rust CPU/allocation recording. The
medium-key native median remains 1.405 s versus stock 1.284 s. Its profiled key-selector sample
is small, while lookup still allocates temporary input strings. The current borrowed-probe
experiment addresses that measured site; it does not claim a whole-job speedup yet.


[Matched local profiles](../benchmarks/results/jvm-aggregate-borrowed-string-probe-2026-10-16.json)
show inclusive JVM aggregate allocation samples fall 4.999 → 4.031 GB and cache-path samples
1.060 → .086 GB, over the same 5M native UDAF rows in each recording. Both contain symbolized
Rust builtin-SUM CPU stacks. Map lookup remains the main sampled Java cost in the aggregate
loop. The subsequent whole-job repeat overlapped external work and is diagnostic only; no
throughput gain or default admission follows from these allocation estimates.


The current STRING index uses the primitive map for Flink string hashes, with full binary-string
equality resolving every collision. An entry chain retains independently owned key views;
removal and checkpoint flushing visit every member. Capacity counts distinct entries rather
than occupied hashes, including the separate NULL entry. This targets the remaining `HashMap`
lookup hotspot in the clean borrowed-probe profile; its throughput remains unproven.

[All four 54-case validation runs](../benchmarks/results/jvm-aggregate-string-hash-index-validation-2026-10-16.json)
pass on both released Flink lines at both capacities. The corpus includes two distinct strings
with the same released Flink hash, fitting and overflowing cache sizes, deletion/reinsertion,
ListViews and repeated restore. Local follow-up profiling was deferred because external Java/
Rust work was live, so no new allocation or timing claim is made for this index.


Earlier v1 throughput comparisons above had asymmetric plan preflight. The [corrected method](../benchmarks/jvm-aggregates.md#symmetric-preflight-correction)
requires EXPLAIN for both engines before timing. Initial v2 STRING native/stock medians are
.637/.721 s at 16 keys, 1.261/1.361 s at 65,536 keys and 2.213/2.048 s for distinct keys;
ranges are retained, including overlaps and the cold-key regression. The primitive-index
[process-scoped diagnostic profile](../benchmarks/results/jvm-aggregate-string-hash-index-profile-2026-10-16.json)
records external work and no timing claims. It confirms the index executes; lookup and
string comparison remain sampled hotspots. Default admission is still open.

The [completed corrected matrix](../benchmarks/results/jvm-aggregates-symmetric-isolated-2026-10-16.json)
now retains both Flink lines, including distinct-key regressions and all mixed Java/Rust
profiles. The batch-scoped UTF-8 view helper is also reused by the separate, default-off
[STRING exit experiment](zero-copy-exit-transpose.md#experimental-borrowed-string-exit).
That reuse does not change the probe's ownership: retained cache keys still refer only to
owned BinaryRow storage, and ordinary Arrow input readers retain their copying behavior.

### Borrow before projecting an owned JVM key

The JVM aggregate's single-VARCHAR-key path also borrows UTF-8 for the released key selector,
including cache misses, cold bypass and disabled caching. The selector's generated projection
immediately returns an owned BinaryRow copy on both released Flink lines. This removes a
temporary Arrow-string heap copy before key materialization; it preserves the required owned
key. The aggregate callback still receives the ordinary reader, so user functions do not
receive these borrowed string views. The shared reader also preserves explicit UTF-16 caches
and falls back to copying for unsupported buffers.

A released-selector ownership test selects a nonleading key, covers NULL/empty/long Unicode
strings, moves the view, mutates the source buffer and retains all keys after batch close.
This change remains inside the default-off aggregate investigation.

[Matched distinct-key profiles and validation](../benchmarks/results/jvm-aggregate-borrowed-key-projection-2026-10-16.json)
retain the preceding revision and candidate with identical query, resources, release library,
exit flag and benchmark fixture. Over 5M native UDAF rows, sampled key-selector allocations
decrease from 1.879 GB to .966 GB and inclusive aggregate-ingest allocations from 6.030 GB to
5.078 GB. Both recordings observed external work (3.94 and 50.71 CPU seconds); all profile
timings are excluded and no CPU speedup or throughput gain is claimed. Heap-state incremental
rehashing remains the largest sampled native-UDAF CPU leaf. Symbolized Rust builtin-SUM control
stacks are retained as well. Sampled allocations are estimates, not copied bytes or memory use.

The normal-capacity selection passes 74 checks on each Flink line. At capacity three, the
aggregate/transpose/JSON selection passes 94 checks on 2.2 and 92 with two compatibility skips
on 1.18; the separate 15-case nested JSON/JVM suite also passes each line. Both changed
production and test sources compile against released dependencies. The completed isolated
comparison below retains the remaining admission failures.

[The completed isolated key-projection matrix](../benchmarks/results/jvm-aggregates-key-projection-isolated-2026-10-16.json)
retains both Flink lines, stock and actual previous-runtime controls, full Java/Rust stacks and
GC logs. Distinct STRING medians improve versus current stock, but medium STRING on 1.18
still regresses, and 2.2 distinct STRING remains slightly slower than its previous-runtime
median. Separate-JVM control drift limits attribution. The cache and aggregate remain default
off; the allocation reduction does not establish a blanket throughput win.
