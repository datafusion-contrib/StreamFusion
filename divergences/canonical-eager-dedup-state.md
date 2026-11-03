# Direct eager deduplication state

Keep the repository's existing dedup algorithms. The Arroyo datastream/operator review found
no equivalent unbounded Flink eager dedup state contract to reuse. Match the released Flink
2.2.1 `SyncStateDeduplicateFunctionBase`, proctime keep-first/last and eager rowtime helpers.
Their state is Boolean presence or the full retained row under `deduplicate-state`.

The proctime keep-last helper stores the row before emitting it, and output can mutate its
stored row kind. That bit changes future duplicate suppression and must cross the boundary.
Event-time ordering additionally retains the timestamp of its selected row. Presence-only
transport does not invent a payload: nullable placeholder columns travel beside the actual key
and original write age, and Java writes only the Boolean stock value.

Follow Comet's `native/core/src/execution/utils.rs` moved C Data ownership pattern. Read bounded
live memory or raw typed RocksDB rows without a native snapshot or replay; preserve expired raw
TTL ages while normal processing retains its own expiry behavior. Validate the entire incoming
view before insertion into a fresh target; hydrate raw persisted keys before duplicate checks.
Close imported buffers before throwing JNI errors. Closing an initialization target after a
resource failure releases its existing memory reservation.

Physical NULL PROCTIME slots are covered with the valid released local-zoned timestamp type.
Those controls pass without changing the runtime representation. A separate synthetic NONNULL
primitive containing NULL is invalid full-row state and must remain rejected. Neither invalid
fixture expectations nor constructor errors receive bug-fix qualification credit.

Normal ordinary/compiled submission now captures the actual released eager function and binds
its state directly. Select the eager runtime path explicitly: a first-row comparator does not
imply the watermark-buffered candidate/timer algorithm. Capture persisted TTL, stock keys,
operator identity and maximum parallelism rather than deriving them from loading-session defaults.
The released eager fixtures prove two imported rows and four new native rows per program; the
qualified mini-batch process cases also require a pending native barrier flush.

Bare-update proctime state is a separate empty projection because the released helper does not
retain rows when both emission flags are off. Keep its stock descriptor, omit unused native rows,
and reject unexpected saved rows without mutating the target.

Equivalent stock sink constraint helpers may have different ordinary-SQL display IDs. Compare the
serialized immutable executor for the exact released class; labels omit settings such as ERROR
versus DROP. Preserve topology/class/uniqueness checks and reject changed definitions.

Buffered/timed state, remaining mini-batch/emission
variants and the full schema/backend/version/connector/failure matrix remain part of
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
