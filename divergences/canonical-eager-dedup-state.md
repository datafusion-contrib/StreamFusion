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

Automatic binding, buffered/timed and mini-batch variants, other emission modes and the full
schema/backend/version/connector/failure matrix remain part of
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
