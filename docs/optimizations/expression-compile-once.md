# Compile expressions once per operator, not per batch

**Applies to:** every Calc/filter operator

Predicates and projections are encoded at plan time and compiled to a DataFusion physical
expression once per distinct input schema; earlier stateless paths re-planned per batch.
A mixed insert/changelog union can require two cached schemas because only one arm carries the
hidden row-kind column. Each handle is released when its owning operator closes.
Removed per-batch query planning from every Calc/filter evaluation.

Large deterministic `AND`/`OR` trees with at least 16 operands are encoded as balanced
binary trees. Operand order is preserved, while compiler depth grows logarithmically
instead of linearly. This lets generated predicates such as TPC-DS Q8 pass the existing
depth limit without increasing it. Dynamic functions and user functions retain their
original expression tree; fallible short-circuit evaluation retains the generated JVM
Calc boundary. The [TPC-DS comparison](../benchmarks/tpcds.md) measures the complete
pipeline rather than assigning an isolated speedup to expression balancing.
