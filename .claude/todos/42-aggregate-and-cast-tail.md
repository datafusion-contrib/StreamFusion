# High-frequency aggregate + expression tail (number↔string CAST, windowed decimal)

**Status:** TODO. Prioritized 2026-07-03 (tier 3 of the coverage push). These are the small,
individually-scoped gaps most likely to silently kill otherwise-simple real queries.

Shipped 2026-07-03:
- `SUM`/`MIN`/`MAX` `DISTINCT` — SUM(DISTINCT) folds a running sum as values enter/leave the
  COUNT(DISTINCT) value set; MIN/MAX(DISTINCT) run as their plain forms (the extreme is
  multiplicity-blind). The same change gated windowed DISTINCT aggregates to fall back — the window
  matchers had admitted them as plain folds, a wrong-results bug.
- **Byte-exact decimal `/` and `%`** — a fused native kernel reproduces Flink's runtime exactly
  (the `BigDecimal` quotient to 38 significant digits HALF_UP, then the rescale to the declared
  `DECIMAL(p, s)` HALF_UP, NULL past `p` digits), on num-bigint since the intermediate can exceed
  i128/i256. The approximate-decimal flag no longer affects arithmetic (it still gates the inexact
  float/double→DECIMAL cast).
- **Decimal `AVG`** (non-windowed single-phase) — SUM's `DECIMAL(38, s)` accumulator plus the exact
  division on emit, reporting `findAvgAggType`'s `DECIMAL(38, max(6, s))`.

Remaining:
- **Window-aggregate decimal `SUM`/`AVG`** — the non-windowed decimal SUM/AVG accumulators exist;
  carry them into the windowed aggregates (one- and two-phase per ticket 41).
- **Number↔string `CAST`** — `CAST(x AS VARCHAR)` / `CAST(s AS INT)` are probably the most common
  expression-level fallback in the wild. Must be byte-exact to Flink's formatting/parsing
  (`BinaryStringDataUtil` / Flink's cast rules — trailing zeros, scientific notation thresholds,
  trim semantics for string→number, overflow → error-or-null per Flink's TRY semantics). Follow
  DataFusion Comet's Spark-exact cast kernels as the structural reference (`~/data/datafusion-comet`
  has the same problem for Spark and solves it with dedicated kernels + parity tests).
  Siblings once the pattern exists: narrowing `VARCHAR(n)` (truncation), `CHAR(n)` space-padding.

**Out of scope here:** UDAF support (a JVM-upcall accumulator bridge — worth its own scoping
ticket if demand appears) and the approximate/idle-TTL declines (deliberate).

**Acceptance:** per item — parity tests against the host over the edge cases (negative scales,
overflow, rounding ties, locale-independence), coverage doc updated in the same commit.
