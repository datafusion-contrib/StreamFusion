# High-frequency aggregate + expression tail (decimal AVG, DISTINCT aggregates, number↔string CAST)

**Status:** TODO. Prioritized 2026-07-03 (tier 3 of the coverage push). These are the small,
individually-scoped gaps most likely to silently kill otherwise-simple real queries.

- **Decimal `AVG`** (non-windowed single-phase, then windowed). Money columns are decimals;
  `AVG(price)` is as common as SQL gets. The blocker is Flink's precision/scale derivation for the
  quotient (`AvgAggFunction` → decimal division semantics); we already model decimal `SUM`
  (i128 at scale s, `DECIMAL(38, s)` overflow→NULL), so the sum half is paid. Byte-exact decimal
  division (below) is the shared primitive.
- **Window-aggregate decimal `SUM`/`AVG`** — the non-windowed decimal SUM accumulator exists;
  carry it into the windowed aggregates (one- and two-phase per ticket 41).
- **`SUM`/`MIN`/`MAX` `DISTINCT`** — only `COUNT(DISTINCT x)` is native. The per-key value set it
  keeps is exactly what the others need; fold the aggregate over the set instead of counting it.
- **Number↔string `CAST`** — `CAST(x AS VARCHAR)` / `CAST(s AS INT)` are probably the most common
  expression-level fallback in the wild. Must be byte-exact to Flink's formatting/parsing
  (`BinaryStringDataUtil` / Flink's cast rules — trailing zeros, scientific notation thresholds,
  trim semantics for string→number, overflow → error-or-null per Flink's TRY semantics). Follow
  DataFusion Comet's Spark-exact cast kernels as the structural reference (`~/data/datafusion-comet`
  has the same problem for Spark and solves it with dedicated kernels + parity tests).
  Siblings once the pattern exists: narrowing `VARCHAR(n)` (truncation), `CHAR(n)` space-padding.
- **Byte-exact decimal `/` and `%`** — native today only under the approximate opt-in. Flink
  derives the quotient scale differently from Arrow; implement the division at Flink's result
  scale/rounding (HALF_UP at the declared scale) on i128 (widening to i256 where the intermediate
  overflows), then retire the approximate flag for these.

**Out of scope here:** UDAF support (a JVM-upcall accumulator bridge — worth its own scoping
ticket if demand appears) and the approximate/idle-TTL declines (deliberate).

**Acceptance:** per item — parity tests against the host over the edge cases (negative scales,
overflow, rounding ties, locale-independence), coverage doc updated in the same commit.
