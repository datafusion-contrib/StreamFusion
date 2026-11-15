# DATE_FORMAT: compile the pattern once, render into a reused buffer

**Applies to:** `DATE_FORMAT` expression evaluation

The default plain-TIMESTAMP numeric path compiles the translated fields once per distinct pattern
per batch and writes into a reused buffer. It derives calendar fields with integer proleptic
Gregorian arithmetic over the full signed-millisecond range. Java `yyyy`/`yy` use year-of-era:
year 0 prints `0001`, year -1 prints `0002`, and a year-of-era above 9999 has a leading `+` for
`yyyy`. This avoids Chrono's smaller date range and differing signed-year formatting. The
timezone-dependent opt-in path retains the Chrono implementation described below.

Historically, chrono's `format(pattern)` re-parsed the strftime pattern inside every `Display`, and the original
loop rendered each row into a fresh `String` — a per-row parse plus a per-row allocation for a
pattern that is a validated literal (the JVM encoder admits only translated literal patterns). A
Fluss-rung profile surfaced it: `StrftimeItems::next` under `spec_to_string`, once per row.

## First round: compile once, reuse the buffer

`CompiledFormat` parses the pattern to owned `Item`s once per distinct pattern per batch (in
practice once), formats with `format_with_items`, and writes into one reused buffer that the Arrow
builder copies from — the same compile-once principle
applies to regex.

Measured (Criterion `date_format/*`, 4096 rows, the Nexmark `yyyy-MM-dd` pattern): the old
formulation (`per_row_parse`) runs 670 µs/batch (~6.1 Melem/s); the compiled path 378 µs
(~10.8 Melem/s) — **1.77× on the hot loop**, kept as an A/B pair in the bench suite.

On the [matrix](../benchmarks.md), `DATE_FORMAT` is a ~10% slice of its queries (q10/q14/q15/q16/q17),
so the end-to-end effect sits inside combined-run noise at 500K events — the loop win is the stable
number, not an end-to-end one.

## Second round: skip chrono's rendering machinery entirely

A 2026-07-12 q17 profile still had `DelayedFormat::fmt` (per-item `core::fmt` dispatch and padding)
as the bulk of the remaining cost. Patterns made only of literals and zero-padded date/time fields —
every Nexmark pattern — now lower once to a digit-writing plan that pushes ASCII digits straight into
the reused buffer. Anything else (and a year outside 4 digits, which chrono prints unpadded) falls
back to `format_with_items` per row, so output stays byte-identical — a Rust parity sweep pins the
two renderings against each other.

Criterion (`date_format/digit_plan` vs `compiled`, same 4096-row batch): 362 µs → 78.7 µs —
**4.6× on the hot loop**, 11.3 → 52 Melem/s.

As with the first round, this is a loop-level win: the end-to-end matrix effect remains inside
combined-run noise at 500K events for the queries that use `DATE_FORMAT`.
