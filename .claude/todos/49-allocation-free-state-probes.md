# Finish the allocation-free state probes and retire the ScalarValue-vintage loops

**Status:** part 1 SHIPPED 2026-07-05. The 2026-07-04 round shipped this technique for the
updating join (`ByteKey` state keys, borrowed-byte probes — q20 +4%, q23 +21% cumulative); the
remaining `OwnedRow`-keyed maps followed.

## 1. Borrowed-byte probes: DONE (2026-07-05)

`group_agg.rs` (group map + emitted changelog keys as borrowed slices), `dedup.rs` keep-last
(key `ByteKey`, payload `Arc<[u8]>` — the `-U` moves the replaced payload out, an ignored stale
row allocates nothing), and `topn.rs` append-only ranker (partition map; a rank>N drop allocates
nothing) all probe by borrowed bytes now, copying only on first insert. Numbers to be quoted from
the post-round matrix rerun (q4/q15/q16/q17/q18/q19 are the touched paths). Remaining in this
family: `LocalGroupAggregator`'s scalar `GroupKey` maps (hot only under mini-batch/tuned mode —
swap to arrow-row + ByteKey when the tuned benchmark column lands).

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
