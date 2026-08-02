# Compile expressions once per operator, not per batch

**Applies to:** every Calc/filter operator

Predicates and projections are encoded at plan time and compiled to a DataFusion physical
expression once against the first batch's schema; earlier stateless paths re-planned per batch.
Removed per-batch query planning from every Calc/filter evaluation.
