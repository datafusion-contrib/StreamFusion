# Preserve stock unbounded accumulators directly

Arroyo separates retained window inputs from execution. An unbounded processing-time OVER fold
needs no retained-input window: its durable result is the generated accumulator. StreamFusion
therefore transfers that live accumulator directly instead of replaying input or materializing
an intermediate native snapshot. Replaying would lose floating-point order and overflowed or
otherwise hidden arithmetic state.

The binding starts from the original released Flink operator and captures its physical fields,
key serializer, retention and identity. It reuses the stock-template route for ordinary SQL so
constant projections cannot create ambiguous owners. Flink's own canonical writer owns the
savepoint container and stock serializer snapshots. Stock restore needs no StreamFusion types.

The native validation entry point shares the import decoder but performs no state installation.
This lets the Java adapter validate all source chunks before applying any of them, preserving
source entries on malformed later chunks or a failed target allocation. A fresh target can retry
a valid immutable source. The JNI boundary follows Comet's moved Arrow ownership: imported C Data
is released before reporting an exception so release callbacks cannot erase the original failure.

The initial scalar binding qualifies BIGINT, FLOAT and DOUBLE arithmetic. Decimal, DISTINCT,
views, event-time unbounded timers and the remaining connector/version/failure protocols are
unfinished work in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569), not
alternative compatibility formats. Benchmarking is explicitly deferred.
