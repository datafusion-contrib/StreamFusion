# Legacy group windows: route `GROUP BY TUMBLE/HOP(...)` onto the native window operators

**Status:** TODO. Prioritized 2026-07-03 (tier 4 of the coverage push).

The legacy group-window syntax (`GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)` / `HOP(...)`) plans as
`GroupWindowAggregate`, which has no native path today — any query using it falls back entirely.
The syntax is deprecated in favor of the windowing TVFs, but enormous amounts of existing Flink SQL
still use it, so this is the best effort-to-coverage ratio among the whole-operator gaps.

**The template already exists:** a legacy event-time `SESSION(...)` group-window routes natively by
reusing the session operator (see the exception in `docs/coverage-and-fallbacks.md` §a). The work
is the same mapping for `TUMBLE`/`HOP`: recognize the legacy plan shape, translate its window spec
and window-property outputs (`window_start`/`window_end`/`rowtime`/`proctime` — the property-order
constraint the SESSION path already enforces) onto the existing native tumbling/hopping window
aggregate operators. Proctime legacy group windows can reuse the processing-time-timer path the TVF
windows already have.

**Scope notes:**
- Same aggregate/type support as the TVF-planned window aggregates (the operators are shared);
  no new native operator expected — this is matcher/exec-node translation work.
- Legacy windows have semantics quirks vs TVF windows (allowed lateness API, early/late fire
  configs); those configs are off by default — decline them rather than modelling them.
- `GroupWindowTableAggregate` (TableAggregateFunction inside a legacy window) stays out — it's the
  UDAF question, not a window question.

**Acceptance:** parity harness runs the same query in legacy and TVF syntax and both match stock
Flink; coverage doc's `GroupWindowAggregate` row updated in the same commit.
