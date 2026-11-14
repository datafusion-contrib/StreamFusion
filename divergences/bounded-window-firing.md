# Bounded synchronous window firing

Arroyo's `crates/arroyo-worker/src/arrow/tumbling_aggregating_window.rs` drives a final
DataFusion stream and collects each returned Arrow batch. StreamFusion follows that output
shape while retaining Flink's synchronous mailbox ownership: the native firing pass calls a
synchronous Arrow receiver for each bounded batch. There is no actor or asynchronous state
mutation beneath Flink, and no continuation survives a successful watermark call.

The JNI handoff reuses the existing Comet-style C Data release/ownership pattern and the
engine's established batch receiver. Output and decode reservations follow their owners and
release on success, receiver failure or cancellation. Comet's bounded producer buffering and
Flink's per-timer firing provide the reference for avoiding a full-result materialization.

Native fixed-window RocksDB keys lead with Flink's key group, so one database iterator cannot
produce the memory backend's global (deadline, end, grouping-key) output order. A k-way merge
of key-group iterators retains an index heap and compares pinned keys instead of copying wide
keys or row payloads. Regional calendar deadlines can reorder local window ends; the firing
pass retains only a deadline inventory before visiting their groups. This storage adaptation
preserves released state encodings and the previous output order rather than replacing our
state with Arroyo's actor-owned execution plans.

Flink can interrupt timer drains through mailbox continuations on versions/configurations
that support them. This change keeps the existing mailbox scheduling contract and bounds
native decode/output frames inside that call. It does not implement such continuations.

Session state is key-major because ingest must see neighboring sessions for merges. Its firing
scan uses the same ordered iterator merge, but batches key/end headers for calendar filtering
and point-decodes only closed payloads. The memory path walks sorted keys and bounded session
frames. Both retain the existing (group key, session start) output order, and preserve pending
sessions and their next processing-time deadline.
