package io.github.jordepic.streamfusion.planner;

/**
 * Marks a physical rel that consumes {@link io.github.jordepic.streamfusion.operator.ArrowBatch}es
 * rather than rows — a native columnar operator, a columnar sink, or the Arrow→row transpose. The
 * transition pass uses this (with {@link ColumnarOutput}) to decide whether an edge needs a
 * row↔columnar transpose: it does only when the producer's output carrier differs from what the
 * consumer expects.
 */
public interface ColumnarInput {}
