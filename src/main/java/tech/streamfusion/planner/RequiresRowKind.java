package tech.streamfusion.planner;

/** A columnar consumer that needs Flink's per-record changelog kind in Arrow. */
public interface RequiresRowKind {
  boolean requiresRowKind();
}
