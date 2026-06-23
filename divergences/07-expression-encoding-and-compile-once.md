# Expression IR: hand-encoded, compiled once, no Substrait

**Kind:** structural — how `RexNode` predicates/projections cross the JNI boundary.
**Diverges from:** nothing (it *follows* Comet); records why we reject Substrait.
**Forced by parity:** the encoding choice is; the compile-once lifecycle is performance.

## The options
Three ways to get an expression tree from the planner to the native engine:

1. **Substrait** — a cross-engine serialization standard (what Gluten uses to
   feed Velox/ClickHouse).
2. **A proprietary encoding** — hand-translate each node to a compact format and
   rebuild the engine's own expression natively (what Comet does, with Protobuf).
3. **No boundary** — plan with DataFusion directly and walk its `Expr` tree (what
   Arroyo does; it is pure Rust with no JVM, so it has no encoding problem at all
   and offers nothing to copy here).

## What we do, and why
We follow **Comet (option 2)**. We hand-translate `RexNode` to a compact pre-order
encoding — parallel primitive arrays (`kinds`, `payload`, `childCounts`) plus typed
literal pools — and rebuild a DataFusion `Expr` natively. Comet uses Protobuf for
the same job; we start with parallel arrays because the op set is small and they
cross JNI as cheap primitive arrays with no new dependency. If the admitted op set
grows toward Comet's scale (nested structs, N-ary functions, casts), switching the
*wire format* to Protobuf is the natural upgrade — it is a maintainability change,
not a performance one, since encoding happens once per operator, not per batch.

We **reject Substrait**, the same call Comet made. Substrait describes expressions
generically; we would still have to map every node onto Flink's exact semantics
(integer division, decimal scale, null and collation rules), which is the entire
value of the project. Its generality buys nothing toward parity and costs a heavy
dependency and a semantic-mapping layer we would own anyway. Parity-first means we
admit ops one at a time behind a matcher gate — the opposite of accepting an open
vocabulary we then have to defend against.

## Compile once, evaluate directly (performance)
Independent of the wire format: the native side compiles the decoded `Expr` into a
physical expression **once**, against the first batch's schema, and caches it on the
operator handle (`createFilterExpression` → handle; `filterExpression` per batch).
Each batch then evaluates that physical expression directly and filters with an
Arrow kernel — no per-batch `SessionContext`, logical→physical planning, or async
stream. This mirrors Comet, whose `PhysicalExpr` is built at plan time and reused.

The earlier stateless filters (`filterBatch`, `filterGreaterThan`) re-planned a full
DataFusion query *per batch*; that path is superseded by the compiled handle. The
cost it removed is recorded in the profiling ticket as the first confirmed
hot-path finding.

## Scope / consequences
- The matcher gates to the admitted op codes and operand types; any un-admitted node
  anywhere in the expression makes the whole Calc fall back. Every admitted op gets a
  parity test before it is turned on.
- The handle holds compiled native state and must be released on operator close, like
  the aggregator handles ([04](04-synchronous-stateful-execution.md)).

## Admitted-op semantics notes (parity edges)
- **Integer `/` and `%`:** DataFusion and Flink (Java) agree for all finite operands —
  division truncates toward zero and modulo takes the sign of the dividend (verified with
  negative dividends, not just positives). Two edges are *not* silent divergences:
  divide-by-zero fails the job on both sides (Flink throws, DataFusion's kernel errors and
  the operator surfaces it — a query that divides by zero fails either way, never a wrong
  answer); and the single pathological `INT_MIN / -1` (and `LONG_MIN / -1`) overflow, where
  Java wraps to `MIN` but DataFusion's checked kernel errors. The latter is the one input we
  do not reproduce bit-for-bit; it is astronomically rare and fails loudly rather than
  silently, so we admit `/`/`%` and flag it here rather than forcing a fallback. (Contrast
  `+ - *`, which use wrapping kernels on both sides and match even on overflow.)
- **`COALESCE`/`NULLIF`:** lowered on the encoder side to the searched `CASE` the host defines
  them as, so they inherit `CASE`'s parity exactly rather than relying on a separate native
  function.
