# When to accelerate: keep columnar chains, avoid lone islands

**Status:** open (design / policy) — now benchmark-informed.
**Source:** user direction; inspiration from DataFusion Comet

**Update:** the "keep columnar chains" *mechanism* is built (ticket 21: columnar flow with
transposes only at boundaries). What remains here is the *policy*: release benchmarks show a lone
stateless island with row endpoints on both sides runs < 1× (bare filter 0.83×), while operators
doing real columnar work win (tumbling 1.21×, columnar copy 3.19×). Decide whether the planner
should *refuse* to substitute an operator that would be an isolated island between two row
endpoints (no adjacent native op, no columnar endpoint), since it cannot beat Flink there.

## Problem
Today the planner substitutes each supported operator independently. Every
native operator transposes RowData -> Arrow on input and Arrow -> RowData on
output. So an isolated native operator surrounded by host operators pays two
transpositions for one operator's worth of native speedup — which can be a net
loss. Worse, two adjacent native operators each transpose at their shared edge
even though the data could have stayed columnar between them.

## What Comet does
Comet runs operators columnar and inserts explicit transition operators
(columnar->row, row->columnar) only at the boundary between accelerated and
non-accelerated regions. It converts **maximal connected subtrees** of
supported operators so transitions happen at the edges of a chain, not per
operator, and it avoids creating tiny "columnar islands" whose transition cost
outweighs the gain. The decision is cost/heuristic- and config-driven.

## What we want
A substitution policy, not just per-node matching:

1. Identify **maximal connected chains** of supported (substitutable) operators
   in the optimized plan.
2. Transpose RowData<->Arrow (or use a native shuffle, see ticket 10) only at
   the chain's boundaries with host operators.
3. Don't substitute a chain whose only member(s) wouldn't beat the transition
   cost — a heuristic/threshold (e.g. min chain length, or a cost model), and a
   config to force/disable.
4. Per the repo's benchmark rule: a single accelerated operator should be
   benchmarked against the transition overhead; if it doesn't win, it should
   stay on the host or only be taken as part of a longer chain.

## Notes
- This reframes the matcher from "replace a node" to "find and replace a
  region", inserting a single ingress transpose and a single egress transpose
  per chain.
- Interacts with ticket 10 (native shuffle): a shuffle inside a chain currently
  forces a transpose; a native columnar shuffle keeps the chain columnar across
  it.
- Reference: Comet's session extensions / columnar transition insertion.
