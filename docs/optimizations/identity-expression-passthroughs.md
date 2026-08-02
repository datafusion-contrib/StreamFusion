# Identity expressions admitted as passthroughs

**Applies to:** the ROWTIME materializer Calc, `PROCTIME()` materialization

Flink's rowtime materializer Calc (`Reinterpret(CAST(rt))` — both value-identity) and `PROCTIME()`
materialization were rejected by the encoder, which kept whole event-time and proctime pipelines on
the host even though neither expression does any real computation.

Encoding them as passthrough columns / a per-batch literal keeps the island whole at zero compute
cost. The proctime value itself is never observed in output — it's an arrival-order signal that gets
projected away — so admitting it as a passthrough carries no correctness risk despite `PROCTIME()`
being non-deterministic in general.
