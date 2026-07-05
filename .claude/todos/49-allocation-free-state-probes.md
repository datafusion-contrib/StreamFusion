# Finish the allocation-free state probes and retire the ScalarValue-vintage loops

**Status:** TODO. The 2026-07-04 round shipped this technique for the updating join (`ByteKey`
state keys, borrowed-byte probes — q20 +4%, q23 +21% cumulative) and for the aggregators years of
churn earlier (arrow-row keys). What's left is the same two migrations applied to the operators
that missed them; all mechanical, all following in-repo precedent.

## 1. Borrowed-byte probes for the remaining `OwnedRow`-keyed state maps

These operators already key state by arrow-row bytes but still allocate an owned key **per input
row** just to probe a map that, in steady state, already contains it (`.row(i).owned()` before
`get`/`entry`). Switch the maps to `ByteKey` (`keys.rs` — `Borrow<[u8]>`, copy only on first
insert), exactly as `updating_join.rs` now does:

- `group_agg.rs` — `keys: HashMap<OwnedRow, GroupKeyState>` (q4/q15/q16/q17, every rung; the
  per-row `.owned()` at the top of the update loop plus the `push` closure's `key.clone()`).
- `dedup.rs` keep-last — `rows: HashMap<OwnedRow, (i64, OwnedRow)>` (q18).
- `topn.rs` append-only ranker — `groups: HashMap<OwnedRow, Vec<TopNRow>>` (q19).

## 2. Retire the `Vec<ScalarValue>` loops (the pre-arrow-row vintage)

Still building a scalar per column per row (`read_key`/`ScalarValue::try_from_array`,
`compare_rows` over scalar rows, `scalars_to_array` emits):

- `over_agg.rs` — all three keyed loops build a `GroupKey` per row. No current Nexmark query is
  bound by it (the matrix OVER queries lower to Top-N), so measure a bench first (ticket 20's
  standing rule).
- `topn.rs` `RetractableTopNRanker` — full ScalarValue rows + per-row `read_key` + `to_vec`
  buffer snapshots per input row. Hit by any Top-N over a changelog input (not in the Nexmark
  matrix; will matter the moment one is).
- `dedup.rs` keep-first — `HashSet<GroupKey> emitted` + per-batch `HashMap<GroupKey, ...>`.
- `exchange.rs` `partition_for_key(GroupKey)` — smallest (0.3–0.9 samples/iter in the profiles);
  swapping changes the internal key→partition mapping, note it when touched (ticket 20's caveat).

This subsumes ticket 20's "scalar `GroupKey` survives in the smaller keyed loops" bullet — that
ticket keeps the measurement rule (swap with a bench showing it pays); this one is the work list.

Acceptance: no `.owned()`/`read_key` per-row allocation in any state-probe hot loop (profile
leaves clean of `sip::`/malloc in the probe paths); parity suites green; per-query numbers quoted
for whichever of q4/q15/q16/q17/q18/q19 move.
