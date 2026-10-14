# DATE_FORMAT / EXTRACT over TIMESTAMP_LTZ: JVM upcall by default, chrono-tz opt-in

## Context
`DATE_FORMAT` and `EXTRACT` (`HOUR`/`YEAR`/…) read the calendar fields of a timestamp. Over a plain
`TIMESTAMP` those fields are the stored wall-clock — zone-independent — and the native functions format
the millis as UTC, matching Flink exactly. Over a `TIMESTAMP_LTZ` (how a Kafka epoch-millis rowtime
arrives, lifted by `TO_TIMESTAMP_LTZ`), Flink first renders the instant in the session time zone
(`table.local-time-zone`) and then reads the fields. The result therefore depends on the zone, including
its DST rules. The native side has no time-zone database, so it cannot reproduce this — historically the
matcher rejected an LTZ argument and the whole node fell back (the Nexmark q10/q14/q15/q16/q17 gap on the
Kafka source).

## Decision
Support the LTZ case with two paths, mirroring how `UPPER`/`LOWER`/`REGEXP_EXTRACT` handle
JVM-vs-native-semantics risk:

- **Default — JVM upcall (byte-parity).** Route the LTZ case through Flink's *own*
  `DateTimeUtils.formatTimestamp(ts, pattern, zone)` / `extractFromTimestamp(unit, ts, zone)` via the
  existing columnar UDF bridge. The pattern/field and the session zone are baked into a small serializable
  `ScalarFunction`; the only column argument is the timestamp, marshalled as epoch millis
  (`NativeUdf.TYPE_TIMESTAMP`). Because it *is* Flink's code, the result is identical by construction — no
  tz-database of our own, one JNI crossing per batch.
- **Opt-in — pure-native `chrono-tz` (`allowIncompatible`).** Convert the instant to the session zone's
  local wall-clock in Rust (`chrono-tz`, the IANA database — the same crate DataFusion Comet uses for
  Spark's zone-aware temporal functions), then format/extract. No JNI hop; faster.

A plain `TIMESTAMP` argument is unchanged — pure-native, no zone, no upcall.

## Why the native path is opt-in (its divergence surface)
`chrono-tz` and the JVM both derive from IANA, but the risk is real and bounded (confirmed against Comet's
own compatibility notes and the IANA/JDK tzdb docs):

- **tzdb-version skew** — the two bundle IANA on independent release cycles, so a specific historical or
  future transition can differ.
- **Far future (> ~2100)** — `chrono-tz` extrapolates DST differently from the JVM (Comet documents the
  same cliff).
- **Deep history / pre-1970** — IANA itself doesn't guarantee accuracy everywhere.
- **Legacy zone forms** — `chrono-tz` rejects `GMT+1`/`UTC+1`/`PST`; the encoder gates the accepted zone
  forms (IANA names, `UTC`, fixed offsets) and falls back on anything else, so we never diverge silently
  on an unparseable zone.

One class of edge case that does **not** apply: the conversion is one-way (UTC instant → local wall-clock),
which is always unambiguous. The DST gap/overlap ambiguity that plagues the reverse direction (parsing a
local string) never arises for these functions.

## Reference
Comet routes the same operation through `chrono-tz` and documents these exact incompatibilities
(`hour`/`date_trunc` over non-UTC, far-future DST). We take the extra step of a byte-parity default (the
JVM upcall) and keep the native path opt-in, consistent with this project's parity-first rule.

## Regional SESSION calendar

Session connectivity now has a local assignment domain for regional LTZ inputs. The Arrow session
state keeps those local bounds, and a batched callback invokes released `TimeWindowUtil` conversion
for epoch deadlines. This uses Flink's JDK timezone rules rather than introducing an independently
versioned native timezone database. The callback owns a scoped global reference and releases JNI
locals within each call, following Comet's host-resource pattern.

The existing Arroyo-inspired per-key session grouping/connected-run structure remains intact.
Lateness is checked after merging candidate bounds; firing checks every session's converted
end independently, so historical non-monotonic local-end deadlines do not become a prefix flush.
RocksDB scans use the same deadline conversion without changing persisted bound/state bytes.
The firing scan also records the earliest remaining deadline; Java rearms it and derives it again
from restored bounds, including clipped/rescaled state. This matches end-minus-one timer firing
and preserves independent-key processing-time recovery.

Regional fixed-grid, TVF, join, rank and dedup consumers share the released calendar contract.
The candidate planner propagates that contract consistently. Shipping still requires the full-job
performance checks in [#325](https://github.com/datafusion-contrib/StreamFusion/issues/325).

## Regional fixed-grid deadlines

Single-phase fixed windows reuse the local-time assignment grid and the batched released-Flink
calendar callback. Bounds remain local in the Arrow and persistent codecs. Each end has its own
epoch deadline: neither a shifted watermark nor a local-end prefix models the 1883 Los Angeles
case. Heap firing selects and orders windows by deadline; the regional persistent path scans and
filters all ends. The fixed-offset prefix path remains intact.

This preserves the Arroyo-style per-bin Arrow accumulator layout while adapting its firing index
to Flink's calendar contract. Unique end conversion is batched across keys and rows. The firing
pass caches the earliest remaining deadline for processing-time scheduling and restore. Existing
plain TIMESTAMP output remains governed by the explicit input-type flag.

The 84 released single-phase calendar/recovery cases and 96 GLOBAL-stage comparisons per line
pass. Regional shared windows retain slice accumulators and keyed pending timers rather than
independent copies per window, preserving released merge/expiration order under historical timer
reordering. Independent LOCAL and complete LOCAL-to-GLOBAL controls now verify delivery and state behavior.
The join/rank/dedup calendar matrix passes 100 cases on each released SDK, covering exact firing,
late rows and heap/RocksDB restoration. Heap replay reconstructs durable rows before reapplying the
epoch watermark because local bounds alone cannot establish lateness before the callback is installed.
The SQL matrix covers all six consumers in fixed and regional zones; checkpoint controls retain
exact bounds and late-row behavior across two completed restores. Full-job performance validation
remains under #325.

## Regional TVF assignment

The stateless TVF reuses native `windows_for` after a batched released-JDK local-time conversion.
It retains the original payload timestamps while appending local bounds; `window_time` is converted
separately with released `TimeWindowUtil.toEpochMills`, not the timer conversion. Scoped JNI frames
and exception propagation share the calendar resource pattern used by the stateful operators.

The 1.18.1 TVF resets output row kinds to INSERT; 2.2.1 propagates them. A released-line compatibility
constant selects output normalization, using a freshly allocated Arrow tag buffer to avoid mutating
shared input buffers. The native kernel still fans out each input tag in its owned output array.
The planner carries one calendar through TVF, aggregates, joins and rank/dedup. Uncovered shapes
and incompatible input calendars retain the existing fallback rules.
