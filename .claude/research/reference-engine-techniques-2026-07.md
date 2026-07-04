# Reference-engine technique survey — 2026-07-04

Companion to `nexmark-operator-profiles-2026-07.md` (the profiling round that motivated it).
Three deep-dives into the locally checked-out reference engines, focused on the execution-layer
techniques relevant to our measured residues (updating-join state churn, Top-N rank maintenance,
multi-DISTINCT aggregate churn, keyed-loop hashing, perimeter transposes). File paths below are
into `~/data/{arroyo,risingwave,proton}`.

# Arroyo findings (agent report, 2026-07-04)

Ranked techniques (paths under ~/data/arroyo/crates/):

1. **`UpdatingCache` intrusive doubly-linked-list TTL** (`arroyo-worker/src/arrow/updating_cache.rs`):
   O(1) touch (bump to tail), O(evicted) expiry by walking list head, generation counter for restore,
   `Key(Arc<Vec<u8>>)` arrow-row keys with `Borrow<[u8]>` so lookups don't allocate.
2. **Retract-vs-batch accumulator split + deferred flush + no-op suppression**
   (`incremental_aggregator.rs`): retractable aggs (SUM/COUNT/AVG) = live sliding accumulator;
   non-retractable (MIN/MAX) = HashMap<Key, {count,generation}> of row-encoded values, re-aggregate
   lazily at evaluate(). Dirty-key set with pre-update value; flush on tick; suppress retract==append
   emissions; append fast-path (whole-batch update_batch when no retract column).
3. **Per-bin background partial aggregation** (`tumbling_aggregating_window.rs`): each window bin owns
   a running DataFusion partial-agg plan fed by channel; process_batch sorts by bin, partitions,
   zero-copy slices per bin; watermark handler only drains/finishes. Driven cooperatively from the
   operator's own select loop (future_to_poll over FuturesUnordered) — no extra tasks.
4. **Paned/tiered sliding-window aggregation** (`sliding_aggregating_window.rs`,
   `TieredRecordBatchHolder`): overlapping HOP windows reuse pane partials (Nexmark q5).
5. **Zero-copy plan reuse** (`arroyo-planner/src/physical.rs` RwLockRecordBatchReader /
   UnboundedRecordBatchReader): compiled physical plan re-executed per batch, inputs injected via
   Arc<RwLock> slots, `reset()`+`execute()` — no replanning, no copies. (We already do the passer
   pattern for joins; check coverage elsewhere.)
6. **Operator chaining into one task with direct-call collectors** (`arroyo-operator/src/operator.rs`
   ChainedOperator/ChainedCollector): no channel hop between adjacent operators.
7. **Row-count-bounded backpressure channel** (`context.rs` BatchSender: bound by rows not batches).
8. **Single select! hot loop, parked-stream checkpoint alignment, MissedTickBehavior::Skip flush.**
9. **Batched lookup join**: in-batch key dedup, one connector.lookup per batch of misses, row-encoded
   moka cache with byte-size weigher (`lookup_join.rs`).
10. **jemalloc global allocator** (off on macOS) — we already ship mimalloc opt-in.

Source batching defaults: source_batch_size 512, linger 100ms (`arroyo-formats/src/lib.rs`,
`arroyo-rpc/default.toml`).

Gaps where Arroyo is beatable: no specialized Top-N (per-instant full DataFusion window exec);
no SIMD beyond stock Arrow; stock tokio (no thread-per-core).

Watermark generator: max event-time via Arrow `max` kernel per batch, but broadcasts watermark only
once per configured interval of event-time progress; idleness coalesced on 1s tick.
# RisingWave findings (agent report, 2026-07-04)

Ranked by likely impact on StreamFusion residues:

1. **View-composition rows (`Chain`/`Project`/`Once`) + single-`Bytes` `CompactedRow` for cached state** —
   kills the per-buffered-row owned allocation in the updating join. Buffer one compact byte blob per row;
   compose output via zero-alloc chained views; materialize only at emit.
   Files: `src/common/src/row/{compacted_row.rs,chain.rs,project.rs}`, `join/row.rs:99-113`.
2. **DISTINCT dedup = visibility-bitmap masking over the chunk + per-key `Box<[i64]>` ref-count vector
   serving ALL distinct calls on the column** — mutate visibility in place, no new rows/ScalarValues.
   File: `src/stream/src/executor/aggregate/distinct.rs:67-197`.
3. **Adaptive `JoinRowSet`: Vec (≤4 entries) → BTreeMap, demote at 2** (`join/join_row_set.rs`);
   **`PrecomputedBuildHasher`** (hash computed once at key serialization, map hasher returns it,
   `hash/key.rs:279`); **stack-allocated `FixedSizeKey<N>`** for fixed-width keys (`hash/key_v2.rs`).
4. **Top-N three-zone cache (low/middle/high `EstimatedBTreeMap<CacheKey, CompactedRow>`)**, boundary
   shifts move ≤1 element per zone; staged **net-diff** (`TopNStaging`/`ChangeBuffer`) cancels
   insert+delete within a chunk before emission. CacheKey = pre-serialized memcomparable bytes.
   File: `top_n/top_n_cache.rs` (zones at :51, staging at :827).
   NOTE: RisingWave never materializes ROW_NUMBER downstream — but our Flink-parity Top-N with rank
   projection must emit rank shifts, so the cascade can't be fully avoided when rank is projected
   (q19); the win is for rank-less Top-N and the state maintenance itself.
5. **Separate degree table** (per-row match count on the other side) so outer/semi/anti NULL-emit and
   retract decisions are an integer read, not a re-probe (`join/hash_join.rs:355-380`).
6. **In-place changelog compaction** (`StreamChunkCompactor` inline mode flips op/visibility bits, zero
   array realloc; `eliminate_adjacent_noop_update` squashes -old/+NULL/-NULL/+new)
   (`common/compact_chunk.rs:42`, `join/builder.rs:166`).
7. **Sequence-based global-LRU managed caches**, one shared memory budget, amortized size accounting
   (report every 4MB) (`cache/managed_lru.rs`, `common/src/lru.rs:301`).

Also: join cache stores value-encoded `CompactedRow` (40B `EncodedJoinRow`), decode-on-read;
`take_state_opt` moves map entry out to work on it, put back after (no clones);
`StreamChunkBuilder` = columnar arrays + ops vec + visibility bitmap (changelog stays columnar);
join output via precomputed index mappings into column builders (`join/builder.rs:27-87`).
# Proton findings (agent report, 2026-07-04)

Ranked for StreamFusion residues (paths under ~/data/proton/src):

1. **`TrackingUpdates` dirty-flag header per group state + separate `retract_pool` arena for old-value
   snapshots, wiped in bulk per finalization; emit walks only dirty groups (or only ingest-touched
   keys)** — finalize scales with changed groups, not table size; retract snapshots are lazy
   (first mutation per group) and arena-bulk-freed. Files: `Interpreters/Streaming/Aggregator/
   TrackingUpdatesData.h`, `MemoryAggregator_ExecuteAndRetract.cpp:143-200`,
   `MemoryAggregator_Convert.cpp:105,374`.
2. **Block-based join state**: retained side = few big columnar blocks (`RefCountDataBlockList`,
   `pushBackOrConcat` merges small blocks to `data_block_size`), hash table maps key →
   `{block ptr, uint32 row}` RowRefs; ref lists arena-allocated in nodes of 7; blocks freed whole by
   refcount; `RowRefListMultiple` (std::list) for retract/PK-override erase. Files:
   `HashJoin/MemoryHashJoin/{RefCountDataBlockList.h,RowRefs.h,MemoryHashJoin.cpp:340-500}`.
   => our updating-join state becomes append-to-Arrow-batch + offset index, block-granular eviction.
3. **`CountedValueMap`** (bounded sorted btree_map<value,count>): O(1) reject when worse than worst at
   capacity; retract = count decrement, erase at zero; freelist-arena string storage — a spec for
   retractable Top-N. Files: `AggregateFunctions/Streaming/{CountedValueMap.h,AggregateFunctionMinMaxK.h}`.
4. **Columnar `_tp_delta` Int8 changelog** (matches our `$row_kind$`) + **watermark as chunk metadata**
   (never a column, never per-row); retract+upsert emitted as whole blocks with block-level tags
   (`setRetract`, `setConsecutiveDataFlag`).
5. **Time-aware Arena** (chunk timestamps; `free(t)` recycles whole chunks below watermark to
   free-lists) + **per-time-bucket arenas** via `TimeBucketHashTable` (outer std::map<bucket, Impl>,
   per-bucket arena; window GC = drop bucket map node + arena, O(bucket) not O(keys);
   `forEachValueOfUpdatedBuckets` finalizes only dirty buckets). Files: `Common/Arena.h`,
   `Common/HashTable/TimeBucketHashTable.h`.

Lower priority: ~60-way per-key-type hash map specialization + saved-hash cells (Arrow/DataFusion
hashing overlaps); two-level promotion at result_size threshold; Hybrid spill variants.

Also notable: join build/probe fully batched (`insertFromBlockImplTypeCase` tight loop;
`AddedColumns.appendFromBlock` columnar assembly with lazy default fills); aggregate `addBatch`
kernels take the delta column (`__restrict`, ALWAYS_INLINE, optional JIT — JIT bypassed when delta
present); changelog-aware right/full join synthesizes null-pad retractions (worked example
`MemoryHashJoin.cpp:1342-1413`).
# Alibaba 5-10x claims (agent report, 2026-07-04)

Provenance of "5-10x":
- **Flash 1.0** (vectorized C++ engine, Flink-compatible; commercialized as Ververica **VERA-X**):
  aggregate-only claims vs Flink 1.19 — >5x at 200M events (5.7x), >8x at 100M (8.4x). No per-query
  breakdown published anywhere. No academic paper (checked PVLDB 18).
  https://www.alibabacloud.com/blog/flash-a-next-gen-vectorized-stream-processing-engine-compatible-with-apache-flink_602088
  https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine
- **Only public per-query table** (Alibaba managed VVR *Java* engine, not Flash): 3.24x aggregate vs
  Flink 1.20.4, 100M events, datagen→blackhole, **mini-batch (2s) + distinct-agg split enabled**:
  q9 8.72x, q19 6.16x, q18 5.97x, q20 5.79x, q15 4.30x, q7 3.77x, q4 3.52x, q2 3.4x, q0/q1 2.5x,
  q5 1.02x (worst — sliding window).
  https://www.alibabacloud.com/help/en/realtime-compute-for-apache-flink/latest/performance-test-of-flink-nexmark-in-realtime-compute
- Alibaba marketing elsewhere says Flash = "3-4x Flink on Nexmark" — inconsistent with 5-10x.
- Explicitly NOT the throughput source: ForSt/disaggregated state (VLDB'25 paper: ops win;
  steady-state Nexmark throughput 75-120% of local RocksDB).

Techniques Flash credits (ranked by their emphasis):
1. Vectorized C++ operators, SIMD, batches ~1000 rows / 32KB.
2. Rewritten built-in string/time functions in C++ ("tens to hundreds of x" per function).
3. Vectorized/batched state access (ForStDB): arena pools + large hash index with SIMD parallel
   lookups (small state); custom LSM + async I/O (large state).
4. C++ arena memory management replacing JVM allocation.
5. Whole-plan native or fallback (Gluten-style "Leno" layer), ~80% coverage; Java UDFs run inside
   the vectorized path without forcing fallback.
6. Older Java VERA tier (the 3.24x table): adaptive planning, operator fusion, mini-batch, distinct
   split.

Implications for us:
- The pattern of their per-query wins (q9/q18/q19/q20 = changelog/stateful 5.8-8.7x) matches exactly
  the queries where we trail on the generator rung — and their gains lean on **mini-batch/batched
  state access**, i.e., amortizing per-key state work per batch — our ticket 41 + the reference
  engines' dirty-key/deferred-flush patterns.
- Their stateless q0/q1/q2 at 2.5-3.4x (vs our 1.1-1.6x generator / 3-3.8x Parquet) shows they also
  pay much less perimeter than our rowwise-generator rung: whole-plan C++ with columnar source-side
  batching. 5-10x on a rowwise perimeter is not what they measured.
- q5 ~1x for them; we're at 1.24x — sliding/hopping windows resist vectorization (Arroyo's tiered
  panes are the known lever).
