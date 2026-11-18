package tech.streamfusion.operator;

/** Keeps batches intact when the planner proves that downstream code cannot observe watermarks. */
public final class NativeColumnarTerminalWatermarkAssignerOperator
    extends NativeColumnarWatermarkAssignerOperator {
  private static final long serialVersionUID = 1L;

  public NativeColumnarTerminalWatermarkAssignerOperator(
      int rowtimeColumn, WatermarkExpression expression) {
    super(rowtimeColumn, expression);
  }

  @Override
  protected boolean preserveBoundaries() {
    return false;
  }
}
