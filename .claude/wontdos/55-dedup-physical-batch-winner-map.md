# Keep-last dedup physical-batch winner map

**Status:** WONTDO (2026-07-13).

Mini-batched keep-last dedup still copies every accepted replacement into durable state even though
only the final winner is visible at the logical boundary. A prototype reduced each large physical
Arrow batch to one row per key before allocating payloads and mutating state. It used the normal
rowtime/arrival-order rules, preserved first-touch order, bypassed batches below 1,024 rows, and
sampled cardinality so mostly-unique batches stayed on the direct path. Rust parity and all nine
dedup SQL/operator harness tests passed.

Criterion liked the hot-key shape: across 4,096 rows and 64 keys, four 1,024-row physical flushes
rose from 39.79 to 49.13 M rows/s (+23%), and one logical bundle made from those physical batches
rose from 43.99 to 57.82 M rows/s (+31%). The matching release+mimalloc 5M-event Nexmark A/B did
not carry the gain end to end. With the prototype disabled, q18 ran at 1.603 M/s immediate and
1.327 M/s mini-batched; enabled immediately afterward it ran at 1.586 and 1.313 M/s respectively.
The mini-batched path therefore changed by -1%, within run noise, while the untouched immediate
path moved similarly. The extra transient map is not justified when transpose, exchange, and flush
costs erase its isolated gain. Reconsider only if physical batches reach dedup without the exchange
perimeter or a profile shows repeated durable payload replacement has become dominant again.
