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
