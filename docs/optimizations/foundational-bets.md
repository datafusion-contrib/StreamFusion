# DataFusion + Rust + Arrow as the data plane

**Applies to:** every native operator

The JVM stays the control plane — planning, coordination, checkpoint orchestration — while record
processing moves to native code operating on Arrow column batches. Columnar batches turn per-record
interpretation into vectorized kernels, Rust removes GC pressure from the hot path, and DataFusion
supplies maintained, optimized compute (accumulators, physical expressions, hash joins, file scans)
instead of hand-rolled kernels.

Every native operator either runs on DataFusion compute or is custom only where Flink parity forces
it.
