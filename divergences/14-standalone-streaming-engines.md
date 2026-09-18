# 14 — What we take (and don't) from standalone columnar streaming engines

Two open-source columnar streaming engines were studied as references for the
changelog operators, alongside Arroyo: **RisingWave** (Rust, Arrow-like
`StreamChunk`) and **Timeplus Proton** (a ClickHouse fork, columnar `Block`s).
Both are *standalone engines* — they own their state, runtime, and checkpoint
coordination — so where they conflict with our guest-accelerator stance we follow
the guest stance (the root-cause rule in the README). This note records what each
confirmed and where we deliberately differ.

## What they confirmed
- **Four-way changelog granularity.** RisingWave carries a separate `Op` array
  (`Insert`/`Delete`/`UpdateDelete`/`UpdateInsert`) beside its columnar data —
  structurally our `$row_kind$` byte column ([divergences/13](13-rowkind-carriage-meta-column.md)),
  and four-way like Flink. Proton carries a two-way `_tp_delta` (±1) and must split
  each block into delta-homogeneous chunks — the same two-way limitation as Arroyo.
  We match RisingWave, the more capable of the two.
- **MIN/MAX retraction via a value→count multiset.** Proton's min/max keeps a
  `CountedValueMap<T>` (a `btree_map<value, count>`); RisingWave materializes the
  inputs. Our per-key `BTreeMap<MinMaxKey, count>` for MIN/MAX retraction is the
  same structure as Proton's.
- **INNER join needs no degree table.** RisingWave keeps a per-side keyed row set
  and only maintains a match-`degree` for outer/semi/anti joins; INNER just emits a
  matched pair per association, with the arriving row's op. Our updating INNER join
  follows this: a per-side keyed multiset, emit the cartesian per match with the
  input row's kind, no degree bookkeeping.

## Where we differ (because we are a guest in Flink)
- **State lives in Flink, not a shared store.** RisingWave pages all operator state
  through an LSM (Hummock) behind an LRU cache; Proton owns in-process maps or
  RocksDB. We keep operator state in-process (Rust) and snapshot it into *Flink's*
  keyed/operator state as bytes. A consequence worth keeping: because the full
  per-key state is in memory (not paged), streaming Top-N can hold the entire
  per-partition sorted set in memory and skip RisingWave's three-tier
  cache/lookahead (`low`/`middle`/`high`), which exists to avoid LSM scans on
  eviction. We keep only the equivalent of `middle`.
- **The updating join probes natively, not via DataFusion.** Our append-only
  interval/window joins delegate the match to a DataFusion `HashJoinExec`
  ([divergences/12](12-joins-delegate-match-own-state.md)) — efficient for a
  time-bounded batch of buffered rows. The *regular updating* join instead keeps a
  keyed multiset and probes incrementally per row, like RisingWave's `JoinHashMap`
  and Proton's `MemoryHashJoin`, because retract correctness needs per-row-count
  bookkeeping a batch hash join does not give. So divergence 12 narrows: time-bounded
  append-only joins delegate; the updating join owns its probe.
  Null-safe regular joins follow Flink's per-key `JoinConditionWithNullFilters` policy:
  retained BinaryRow keys already encode NULL identity, so ordinary-equality fields alone
  suppress a probe on NULL. Unlike Comet's Spark planner rewrite or a whole-join DataFusion
  NullEquality flag, this preserves mixed ordinary/null-safe keys without adding synthetic
  key columns. The mask is immutable operator configuration passed through JNI on create and
  every restore route; snapshot row bytes and Arrow ownership are unchanged.
  Keyless INNER joins over insert-only inputs reuse the same regular-join multiset and probe,
  with empty key arrays and Flink's singleton distribution. This deliberately extends the
  established incremental state path instead of adding an Arroyo-style batch execution plan:
  duplicate counts, mini-batch flushing, state TTL and restore already share Flink's contract.
  There is no invented equality key, broadcast build side or output-cardinality limit.
- **Row↔Arrow transpose at host edges.** RisingWave (`StreamChunk`) and Proton
  (ClickHouse `Block`) are columnar end to end; we transpose to/from Flink `RowData`
  at native↔host boundaries ([divergences/08](08-columnar-flow-transitions.md)),
  which is the source of our sub-1× row-fed operator numbers. The columnar-flow work
  is the path to parity there.
- **Processing-time first-N uses Flink's counter state.** The consulted Arroyo worker and
  planner do not provide this arrival-ordered rank operator. We follow Flink's
  `AppendOnlyFirstNFunction`: one integer per partition, written only for an accepted arrival,
  with the existing native keyed-state, TTL and checkpoint infrastructure. Output gathers
  selected rows from the input Arrow batch; neither sorting nor retained payload rows are needed.
  JNI ownership and exception handling follow the same Comet-derived bridge as the other operators.

- **Retracting OFFSET retains Flink row kinds and independent counts.** The consulted Arroyo
  datastream exposes windowed Top-N descriptions, but its worker has no matching four-kind
  retracting OFFSET operator. We follow released Flink 2.2.1's `RetractableTopNFunction` cascade.
  Its heap backend aliases emitted and retained rows, so an emitted kind affects later equality;
  sort-key counts still decrease after failed removals. Compact row bytes remain immutable, with
  mutable kind metadata and the independent count on each tie group's first row. Native RocksDB
  persists the same logical state as native memory, following the established dedup choice to
  preserve heap-backend behavior across native backends. Legacy snapshots infer INSERT kinds
  and list-length counts; new snapshots retain the additional metadata. The planner's UPDATE_BEFORE flag travels through create and restore on both backends;
  ownership and exception handling retain the Comet-derived JNI pattern, and retained metadata
  uses the existing native memory budget.

- **Variable update-fast Top-N follows Flink's retained-key contract.** Arroyo's consulted
  `arrow/window_fn.rs` executes window functions over watermark-delimited batches; it has no
  equivalent continuously updating unique-key rank. We extend the existing columnar ranker
  with released Flink 2.2.1's variable-range admission threshold and overflow tie-group retention.
  A non-null bound proven invariant within its partition can be read from the Arrow row;
  retained unique keys, row TTL and checkpoint bytes reuse the existing state representation.
  The bound-column index is immutable configuration on every create/restore route, following
  Comet's task-scoped JNI handles. This adds no row callback or alternate ownership path.
