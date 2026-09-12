# Columnar Paimon merge engines with released Java option parsing

The existing level-0 writer now supports first-row, partial-update and common aggregation functions,
user sequence fields and rowkind.field. The checkpoint, file layout, compaction and commit boundary
remain the one described in [32](32-paimon-pk-l0-through-the-compactor-hook.md).

Before implementation we consulted Paimon Java's released `release-2.0.0-rc10` implementations:
`SortBufferWriteBuffer`, `ReducerMergeFunctionWrapper`, the four merge functions and per-field
aggregators, `RowKindGenerator`, `RowKindFilter`, `UserDefinedSeqComparator` and `DefaultValueRow`.
We also consulted paimon-rust's `table/sort_merge.rs`, `table/aggregator/` and configuration modules,
and Comet's `NativeUtil.scala` for C Data transfer ownership.

paimon-rust's merge and aggregator interfaces are internal and tied to its table/reader types.
Its current reducers also deliberately exclude retract behavior covered by Java. Importing that
subsystem would require its schema, error, reader and projection infrastructure while still needing
Java parity fixes. We therefore use its column-cell selection approach as inspiration: ordinary
value merges retain indices into Arrow columns; only arithmetic, boolean results and string
concatenation materialize scalar values. One Arrow gather per output column constructs the merged
batch, including nested values, without converting the input into Java rows. No Rust Paimon
dependency or source files are imported.

Released Java Paimon validates the configuration and resolves the effective per-field function.
The connector serializes only the resolved indices, functions and flags for Rust. Defaults are
parsed by Java once per bucket and transferred as a one-row Arrow batch. Their buffers stay owned
by the native buffer and are released on close. Defaults that affect partition/key routing decline
before entering the native topology.

Several details intentionally follow the released writer rather than a simplified merge model:

- Sorting is by key, optional user sequence and arrival number; user sequence never replaces the
  arrival number stored in files. Sequence ties use the arrival number.
- Singleton groups bypass the reducer and retain the input kind, as Paimon's reducer wrapper does.
- Partial updates visit fields in schema order and update sequence columns at the same points as
  Java. Retracts, empty groups, reversed field aggregation and delete resets follow that order.
- Aggregate state resets per key. Removing a record resets the values, but retains stateful
  first-value aggregator initialization, matching Java.
- Integer arithmetic checks overflow. Decimal sum preserves Java's compact-long fast path and
  its precision-checked BigDecimal fallback. Unsupported function/type combinations decline.

The exact supported functions and remaining fallbacks are maintained in
[the connector coverage page](../docs/connectors/paimon.md). Batch sinks remain stock Paimon.

`sequence.field` is admitted with deduplicate only. A twin-table experiment with aggregation,
out-of-order user sequences and listagg produced identical level-0 sequence/count metadata but
different final concatenation order under automatic compaction. Each reduced file carries one
representative user sequence; changing which files compact together can reorder the values already
combined within those files. Forcing both twins to compact at every checkpoint made the results
agree, but would change the user's compaction policy. We therefore retain the stock writer for
user-sequence partial/aggregation combinations instead. Partial-update's per-field sequence groups
remain supported. The remaining coverage is tracked in
[issue #47](https://github.com/datafusion-contrib/StreamFusion/issues/47).
