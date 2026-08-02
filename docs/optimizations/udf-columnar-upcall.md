# UDFs via a columnar JVM upcall

**Applies to:** user `ScalarFunction`s the native engine can't implement itself

A user `ScalarFunction` the native engine can't implement itself runs *inside* the island instead of
falling the whole query back to Flink: the argument columns are packed into one batch, exported over
the C Data Interface, evaluated by the real function on the JVM, and the result column imported back
— one JNI crossing per batch, never per row. The design is modelled on Comet's `JvmScalarUdfExpr`.

Because Flink's own code computes the values, the result is byte-identical to Flink by construction
— there is no reimplementation to diverge.

Functions are serialized into the operator and registered per-task at `open()`, so this survives
distributed execution, where the UDF instance must be reconstructed on each task's JVM rather than
shared from the planner.
