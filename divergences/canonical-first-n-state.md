# Stock-compatible first-N counters

Reference review: Arroyo has windowed `TumblingTopN` and `SlidingAggregatingTopN` declarations in
`crates/arroyo-datastream/src/lib.rs`, but no equivalent unbounded arrival-ordered first-N counter.
Keep the existing native per-key counter and match the released Flink 2.2.1
`AppendOnlyFirstNFunction`: `counterState` is an INT value, optionally wrapped with its original
TTL write timestamp. Rejected arrivals do not refresh it. Payload rows and sorting structures
are absent from this state contract.

The JNI bridge follows Comet's `native/core/src/execution/utils.rs` C Data ownership pattern.
Actual key bytes, count and write age travel in bounded batches directly from live memory or
self-contained RocksDB values. Canonical RocksDB iteration bypasses read expiry while ordinary
reads keep it. No intermediate native snapshot or reconstruction from visible output is involved.
Java consumes moved Arrow buffers synchronously; native import drops borrowed batches before
installing JVM exceptions.

Whole-view restore requires an empty native target, as the backend factory already supplies for
stock canonical initialization. Rejecting a nonempty target avoids merging a stock view with
pre-existing native counters. Validate the complete stock view before native insertion. Per-batch validation rejects invalid
schemas, too-short/non-INSERT keys, negative counters and duplicates before mutating that batch;
raw RocksDB hydration catches duplicates written by earlier chunks. A later invalid stock chunk
cannot leave an earlier valid chunk imported. Resource failure aborts initialization, and closing
the failed target releases its prior off-heap reservation. Receiver failure leaves native state
available for export retry.

Normal planning captures the actual released first-N function and stock schema/identities rather
than inferring state from SQL text. Ordinary wide root-key layouts retain their existing native
route until a complete key-layout migration is qualified. Other rank contracts, broader types,
backends, versions, full-job TTL expiry and connector/failure/delivery guarantees remain required
by [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
