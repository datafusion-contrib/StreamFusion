# Window aggregate

**Status:** Native, both event-time and processing-time, with the gaps enumerated below.

This page covers the windowed `GROUP BY` aggregate — `TUMBLE`/`HOP`/`CUMULATE`/`SESSION`, single-phase
and the two-phase local/global split — and the windowing-TVF operator that assigns each row to its
window(s) ahead of a downstream consumer (an aggregate, a [window join](joins/window-join.md), or
window Top-N/dedup).

## Legacy group windows

The deprecated `GROUP BY TUMBLE(...)` and `GROUP BY HOP(...)` syntax is native for both event time
and processing time, reusing the same single-phase fixed-window operator as TVF-planned aggregates.
Legacy event-time `SESSION(...)` is native too. Legacy group windows have no offset and never use
Flink's two-phase local/global optimization.

The auxiliary properties retain Flink's legacy layout and types: start and end are plain
`TIMESTAMP(3)`, rowtime is the window end minus one millisecond, and the internal proctime marker is
null before the outer Calc materializes the current clock. Queries that select no auxiliary window
property are native as well.

## Event-time assignment

`TUMBLE`, `HOP`, and `CUMULATE` are native only at **zero offset**; `SESSION` needs no offset. The
window bounds render differently depending on the rowtime attribute's type: in the session time zone
for a local-time-zone attribute, in UTC (the raw wall-clock value) for a plain `TIMESTAMP`.

## Processing-time (proctime) assignment

`TUMBLE`/`HOP`/`CUMULATE` are native on proctime: each row is assigned to the window(s) covering the
operator's current processing-time clock and fired on a processing-time timer. `HOP` and `CUMULATE`
leave several windows open at once, so the timer chains — each firing emits the earliest-ending open
window and schedules the next slide boundary, until the clock has passed the latest open window's end.
This requires **slide divides size**, so every window end lands on a slide boundary.

`SESSION` is native on proctime too: the gap is measured on the processing-time clock, and each batch
registers a cleanup timer at `now + gap` — the earliest the session could close with no further input.
A later element extends the session (merged in the native aggregator) and registers its own later
timer, so a firing emits only the sessions the clock has truly left behind by a full gap.

Proctime support is currently **single-phase only**: a single-phase `TUMBLE`/`HOP`/`CUMULATE` whose
slide divides its size, or a single-phase `SESSION`. The two-phase local/global split is not yet on
the processing-time-timer path.

Because proctime results depend on wall-clock timing, they are non-deterministic — routing and
execution are tested, but the result is not byte-compared against Flink.

## Windowing TVF (window assignment)

The windowing TVF assigns rows to windows the same way as the aggregate above — by rowtime for
event-time, by the processing-time clock instead of a rowtime column for proctime — and is native
under the same **zero-offset** `TUMBLE`/`HOP`/`CUMULATE` restriction; both its event-time and
proctime assignment paths are native.

A downstream [window join](joins/window-join.md) or window Top-N/dedup consuming the TVF's output
closes windows on a chained processing-time timer (the same next-slide-boundary model described
above) rather than a watermark, under the same slide-divides-size constraint — see those operators'
own pages for their admission conditions.

## Matcher declines

- Window not event-time `TUMBLE`/`HOP`/`CUMULATE` (zero offset) over a local-time-zone or plain
  `TIMESTAMP` rowtime.
- Proctime: anything other than a single-phase `TUMBLE`/`HOP`/`CUMULATE` with slide dividing size, or
  a single-phase `SESSION` — the two-phase local/global path isn't yet native on proctime.
- `HOP` slide / `CUMULATE` step that doesn't divide the window size.
- Legacy row-count `TUMBLE`/`HOP` windows from the Table API.
- Legacy early/late firing or allowed lateness.
- A legacy group window over retracting or updating input.
- Legacy proctime `HOP` when the slide does not divide the size. Event-time legacy `HOP` supports
  non-dividing and gapped windows.
- Legacy fixed-grid `TUMBLE`/`HOP` over `TIMESTAMP_LTZ` event-time or proctime unless the session
  zone has one fixed post-1970 offset that is an integral multiple of the window slide. Zones with
  post-1970 transitions fall back because Flink assigns and fires on a DST-aware local-time grid.
- Legacy processing-time `SESSION`.
- Key type outside bigint/int/string/boolean/date/timestamp/decimal.
- A value type/aggregate mismatch.
- `AVG` under the two-phase split — its `(sum, count)` buffer spans two positional partial columns.
  A single-phase `AVG` as a lone aggregate is native.
- A **windowed `DISTINCT` aggregate** (`SUM(DISTINCT …)` etc. inside a window) — it dedups per window,
  which the native window operators' every-row fold would over-count. Non-windowed `DISTINCT` is
  native; see [GROUP BY](group-by.md).

A **zero-aggregate grouping-only window** (`GROUP BY key + window`, no aggregate function) is *not*
one of the gaps above — it's a windowed distinct, and is native (single- and two-phase), emitting one
row per `(key, window)`. See [GROUP BY](group-by.md) for how the non-windowed case handles `DISTINCT`.

## Idle-state TTL

Flink applies no idle-state TTL to window operators — `table.exec.state.ttl` changes nothing here;
windows are bounded by their own firing and eviction instead. Contrast with [`OVER`](over.md), which
does run TTL natively across all three of its frame shapes. See [Configuration](../configuration.md)
for the TTL flag surface.
