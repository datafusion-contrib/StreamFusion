# Keep interval cache clocks separate from SQL payloads

Arroyo's `crates/arroyo-worker/src/arrow/join_with_expiration.rs` keeps each Arrow input and
its time-indexed state ownership separate. StreamFusion retains that join structure, but the
released Flink processing-time interval cache stores original rows under an independent arrival
clock. Replacing the SQL timestamp payload with the clock loses state, including Flink's physical
NULL PROCTIME placeholder, and cannot produce the original stock cache.

Canonical-bound processing-time interval operators therefore append one private BIGINT clock to
the native physical schema. Matching and expiration use it; output strips it. The direct canonical
visitor writes the original row and matched bit to Flink's tuple list and the clock to its map
bucket. Residual references are translated once to account for the private left clock column.
Ordinary unbound operators retain their existing layout. Native checkpoint sections remain
self-describing Arrow IPC and restore the bound schema before importing records.

The JNI handoff follows Comet's ownership pattern in `native/core/src/execution/operators/copy.rs`:
retained Arrow state owns its buffers after the input export is released. No native snapshot or
input replay is used to make a canonical savepoint. Real cleanup deadlines rearm idle callbacks;
EOF does not invent a future processing-clock advance. See the [interval coverage](../docs/operators/joins/interval-join.md)
for qualified cases and remaining #569 work. Benchmarking is deferred.
