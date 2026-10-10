package tech.streamfusion.planner;

import org.apache.flink.table.types.logical.LogicalType;

/** Additional storage restriction for operators whose retained rows use Arrow's row codec. */
final class ArrowRowTypeSupport {
  private ArrowRowTypeSupport() {}

  static boolean supports(LogicalType type) {
    switch (type.getTypeRoot()) {
      case MAP:
      case MULTISET:
        return false;
      default:
        return type.getChildren().stream().allMatch(ArrowRowTypeSupport::supports);
    }
  }
}
