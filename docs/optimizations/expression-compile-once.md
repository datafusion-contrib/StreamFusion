# Compile expressions once per operator, not per batch

**Applies to:** every Calc/filter operator

Predicates and projections are encoded at plan time and compiled to a DataFusion physical
expression once per distinct input schema; earlier stateless paths re-planned per batch.
A mixed insert/changelog union can require two cached schemas because only one arm carries the
hidden row-kind column. Each handle is released when its owning operator closes.
Removed per-batch query planning from every Calc/filter evaluation.
