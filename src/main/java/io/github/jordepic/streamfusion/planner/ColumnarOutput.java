package io.github.jordepic.streamfusion.planner;

/**
 * Marks a physical rel that produces {@link io.github.jordepic.streamfusion.operator.ArrowBatch}es
 * rather than rows — a native columnar operator, a columnar source, or the row→Arrow transpose. The
 * transition pass uses this (with {@link ColumnarInput}) to decide whether an edge needs a
 * row↔columnar transpose.
 */
public interface ColumnarOutput {}
