# RocksDB element-granular state for multiset aggregates

**Status: rejected on benchmarks (2026-08-25).**

Group aggregates with MIN/MAX retraction or DISTINCT (aggregate kinds 1, 2, 7, 9) keep per-key
multisets. They are the one state shape deliberately left on the RocksDB snapshot-blob path after
every other operator moved to per-key typed stores.

Two element-granular designs were built and measured (Nexmark state-backend A/B, 500K events,
parallelism 2, exactly-once Kafka, vs Flink RocksDB; control = blob path at the same commit):

1. **Companion element tables with per-bundle prefix-scan hydration** (one RocksDB row per
   (group key, element), whole-set hydrate per touched key): q15 native 1.44s → 2.36s,
   q16 1.42s → 3.31s.
2. **Point-probe redesign** (multi_get over only the batch's elements, running totals moved into
   the main row, extreme-retraction recovery via one memcomparable prefix seek, delta-only
   writes): q15 2.38s, q16 4.62s — worse still on q16, because many distinct aggregates times
   every batch element out-reads the resident fold regardless of granularity.

Control at the same settings: q15 3.39x / q16 3.97x vs Flink (native ~1.42-1.44s).

The workload shape is the reason, not the implementation: distinct-heavy queries are few hot keys
with large sets, for which a resident fold plus a bounded blob checkpoint is structurally optimal —
every element read RocksDB can serve, the resident set serves for free. Flink's own MapState-based
distinct state pays the same per-element costs, which is part of why we beat it 3-4x here.

Reopen only with (a) a benchmark showing blob-checkpoint cost dominating on a real multiset
workload (very large distinct state per key group with frequent barriers), and (b) a design that
keeps hot sets resident between barriers while bounding memory — without reintroducing the
issue-#26 second-write-buffer anti-pattern. The reverted implementation (journals, companion
tables, point probes, extreme reseek) lives in this repo's history on 2026-08-25 if needed.
