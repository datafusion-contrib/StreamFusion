# Shared native sources (read the topic once per query, not once per branch)

**Applies to:** any query joining two or more views over the same underlying topic (q3, q4, q8, q9,
q20 in Nexmark)

## The problem

Nexmark carves its `person`/`auction`/`bid` views out of one interleaved event topic, so every query
joining two views plans two scans of the same table. Flink's sub-plan reuse merges them — one read,
one decode — but that reuse is digest-based, and every native rel deliberately carries a
digest-unique barrier: an Arrow batch is handed to exactly one consumer, which closes its off-heap
buffers after reading, so a blindly merged native subtree would fan one batch to two consumers and
the second would read freed memory. The consequence, exposed by a per-thread q3 profile, was two
full native Kafka sources per query — twice the topic read, twice the JSON decode — the whole q3
loss (its join is under 2% of CPU).

## The fix

The substitution pass now merges identical native sources itself, the way the streaming engines that
faced this same problem do:

- Arroyo dedups source nodes by name and fans out `Arc<RecordBatch>` clones.
- RisingWave rewrites any source referenced twice into one shared `StreamShare` node.
- Flink's own `SubplanReuser` is the host-side precedent this mirrors on the native side.

Semantically identical sources — same options, schemas, and watermark — collapse into one instance
under an explicit share node carrying the branch count. At runtime the share operator declares that
count on each batch, and every chained consumer's `root()` take returns its own zero-copy view over
the same retained buffers (Arrow's buffer reference counts; the split-and-transfer share idiom), so
each branch keeps its usual read-then-close contract and the buffers free on the last close. The
single-consumer hand-off is unchanged, and sources whose branches push down different projections
stay separate (they decode different columns; sharing the union projection is a possible follow-up).
[`streamfusion.plan.shareSources=false`](../configuration.md) restores per-branch sources.

## Measured

(q3, exactly-once Kafka in/out, 2M events, parallelism 4, interleaved same-binary A/B, two legs per
side): sharing lifts native off-mode throughput from ~1.28 to ~1.98 M events/s — **~1.55× faster** —
moving q3 vs Flink from ~0.98× (the one consistent headline loss) to **1.55–1.76×**, and the
mini-batch-on cell from ~0.95× to **1.34–1.50×**.

The same dedup applies to every multi-view query — q4, q8, q9, and q20 all join two views of the one
topic; their heavier downstream work amortized the doubled read, but they stop paying it all the
same.

See [Benchmarks](../benchmarks.md) for the full A/B methodology.
