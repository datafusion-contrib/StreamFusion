# Top-N payloads as `Arc<[u8]>`

**Status:** WONTDO (2026-07-13).

Top-N currently stores payloads as `Arc<OwnedRow>`. A prototype used `Arc<[u8]>` instead, removing
the separate `OwnedRow` allocation while retaining shared payload identity and vectorized decode.
Correctness passed all 10 append-only and retracting SQL harness tests.

The synthetic append-only benchmark improved substantially (logical membership diff +28%, physical
diff +22%, logical rank diff +12%), but retracting immediate/physical paths regressed about 4%, and
the balanced 5M-event guardrail did not carry the gain end to end: q19's direct mini-batch ratio fell
from the prior 1.87x to 1.64x (0.677 M/s off, 1.113 M/s on in the prototype run). A slice `Arc` is a
wider fat pointer, and removing one allocation is not sufficient to offset that cost across shared
ranker machinery. Keep `Arc<OwnedRow>` unless a future design also changes the sort-key/state layout
and wins both the retracting Criterion cases and q19 Nexmark.
