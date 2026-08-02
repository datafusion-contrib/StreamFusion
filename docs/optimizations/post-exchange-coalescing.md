# Post-exchange batch coalescing

**Applies to:** GROUP BY aggregate, both Top-N rankers, the updating join, keep-last dedup, the
changelog normalizer

## The problem

At parallelism p the columnar exchange splits every source batch into per-channel sub-batches, so a
keyed operator sees batches roughly p× smaller than the source emitted — and a changelog operator
feeding another keyed operator compounds the fragmentation: in the 2M/p=4 q4 pipeline the second
aggregate processed 26-row batches at ~71× the p=1 batch count. The per-batch fixed cost (JNI
crossings, per-call setup, per-batch emission) dominated wall time, and every task thread sat
starved (see [Benchmarks](../benchmarks.md), "Why the off-mode changelog shapes stop scaling").

## The fix

Every keyed changelog operator now re-assembles processing-sized batches in front of the native
push: sub-batches buffer until a row target ([`streamfusion.exchange.coalesceRows`](../configuration.md),
default 4096) and merge in one native `concat_batches` call; watermarks, checkpoint barriers, and end
of input always flush first, and a processing-time backstop
([`streamfusion.exchange.coalesceLatencyMs`](../configuration.md), default 50 ms) bounds the wait on
trickle streams — the role Flink's own mini-batch `allow-latency` plays. The two-input join drains
the opposite side before buffering a side, so the cross-side arrival order the join changelog
depends on survives coalescing exactly. Only physical chunking changes: every operator still emits
its per-record cascade, so the off-mode byte-parity contract is untouched (the parallelism-2 shuffle
parity test runs its aggregate through an asserted-real merge).

## Measured

(2M events, parallelism 4, Kafka source → blackhole ladder, interleaved same-binary A/B, two legs
per side): q4 — the two-aggregate + join chain where the collapse compounds — runs **1.59× faster
with coalescing** on json (1.95 → 1.23 s; 1.45× avro, 2.06× protobuf), moving q4 vs Flink from
~1.06× to **1.78×** on json; the row-fed q4 variants gain ~2.5× (5.5 → 2.2 s). q3/q19 sit within
cross-leg noise.

The remaining off-mode lever is the source-side batch floor (a p=4 consumer's ~950-row polls,
quartered by the split before the first operator).
