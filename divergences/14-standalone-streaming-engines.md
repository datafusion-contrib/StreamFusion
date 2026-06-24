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
- **Row↔Arrow transpose at host edges.** RisingWave (`StreamChunk`) and Proton
  (ClickHouse `Block`) are columnar end to end; we transpose to/from Flink `RowData`
  at native↔host boundaries ([divergences/08](08-columnar-flow-transitions.md)),
  which is the source of our sub-1× row-fed operator numbers. The columnar-flow work
  is the path to parity there.
