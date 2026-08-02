# Delegate joins to DataFusion's hash join

**Applies to:** time-bounded joins (interval join, window join)

The time-bounded joins buffer and evict, but the match itself runs as a `HashJoinExec` over the
buffered batches (Arroyo's split), putting the O(n·m) work on a vectorized, maintained join — the
join benches run at **20–40 M elements/s**.

Reusing one `TaskContext` instead of rebuilding a `SessionContext` (and its whole function
registry) per pushed batch later cut the join hot loops roughly in half: interval join **115 → 63
µs**, window join **184 → 130 µs** per 4096-row batch.
