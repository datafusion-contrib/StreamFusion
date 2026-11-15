package tech.streamfusion.planner;

import org.apache.flink.table.types.logical.LogicalType;

/** Additional storage restriction for operators whose retained rows use Arrow's row codec. */
public final class ArrowRowTypeSupport {
  private ArrowRowTypeSupport() {}

  public static boolean supportsFields(
      org.apache.flink.table.types.logical.RowType row, int[] fields) {
    for (int field : fields) if (!supports(row.getTypeAt(field))) return false;
    return true;
  }

  public static boolean supports(LogicalType type) {
    switch (type.getTypeRoot()) {
      case MAP:
      case MULTISET:
        return false;
      default:
        return type.getChildren().stream().allMatch(ArrowRowTypeSupport::supports);
    }
  }
}
