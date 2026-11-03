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
This is an explicit compatibility state mode. Existing unbound rank paths retain their current
behavior; automatic planner binding remains open.

Arroyo has windowed Top-N declarations but no corresponding unbounded retracting operator to reuse.
Flink's released operator is the behavioral reference; Comet's moved Arrow C Data ownership is the
JNI reference. The JNI initialization entry selects the state model; direct transfer reuses the established
columnar ownership/exception boundaries.

Reference traces cover stock canonical restore and native memory/RocksDB checkpoint continuation.
They compare raw presence/counts/kinds/independent ages, with live-state comparison across RocksDB
expiry cleanup; exact raw native checkpoint round trips remain asserted. This state mode is
functionality groundwork. Benchmarking is explicitly deferred under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569), which remains open for wider
canonical schemas, automatic admission and full qualification.


## Direct stock projection

A bounded Arrow protocol transfers list/count-map presence, independent typed sort keys, actual
partition keys, counts/bounds, original ages and data row kinds. Metadata-only keys do not need a
fabricated row. The Java writer uses stock row/list/SortedMap serializers and the actual released
serializable comparator; its snapshot can restore with StreamFusion classes denied. The stock
backend is a transient canonical projection, not a native snapshot conversion artifact.

A whole-source validation pass and empty-target check precede insertion. Validate non-null fields
before primitive serializers/selectors can turn an invalid NULL into a zero. Source state is only
cleared after complete success. Native frames validate metadata/keys and prior chunks before their
own mutations, release read-through residency on errors, and use the established moved-FFI boundary
to release owners before throwing. Memory denial leaves source state for a fresh target, and failed
output callbacks preserve their original exception plus the native state for another export.

Export reserves transient Arrow and decoded persistent state. Raw canonical reads bypass expiry;
TTL-disabled metadata suppresses checkpoint-migration clocks without changing native timestamps.
The initial direct qualification covers BIGINT rows/one ascending BIGINT sort, independent TTL and
bounds, OFFSET, both stores, native checkpoints, no-input re-export and actual stock continuation.
Automatic binding and complete-job/schema/version/connector qualification remain in #569.
