# Preserve independent retracting rank state

Released Flink 2.2.1 uses independent sort-key row lists, a sorted count-map value and an optional
first-observed variable bound. Their presence, contents and TTL write ages cannot be inferred from
one another. A matched last-row removal still leaves a present empty count-map value. Unmatched
retractions, and valid heap hidden-rank OFFSET cascades that mutate retained row kinds, can leave
rows without corresponding count entries even with TTL disabled.

The compatibility foundation therefore uses independent native structures rather than exporting
the existing flattened buffer and deriving counts. Per-record processing mirrors the released
algorithm, including heap row aliases versus RocksDB copies on reads/writes. Immutable checkpoint
capture deep-copies mutable kind headers while sharing immutable payload bytes. New native state
has a checked versioned envelope; legacy row buffers cannot be silently promoted into this model.
This is an explicit compatibility state mode, without new planner admission or a stock canonical
adapter yet. Existing unbound rank paths retain their current behavior.

Arroyo has windowed Top-N declarations but no corresponding unbounded retracting operator to reuse.
Flink's released operator is the behavioral reference; Comet's moved Arrow C Data ownership is the
JNI reference. The new JNI initialization entry only selects the state model, and existing
columnar ownership/exception boundaries remain in use.

Reference traces cover stock canonical restore and native memory/RocksDB checkpoint continuation.
They compare raw presence/counts/kinds/independent ages, with live-state comparison across RocksDB
expiry cleanup; exact raw native checkpoint round trips remain asserted. This state mode is
functionality groundwork. Benchmarking is explicitly deferred under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569), which remains open for direct
canonical descriptor transfer, admission and full qualification.
