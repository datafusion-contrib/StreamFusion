package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** Shared Flink expression type and builtin shape facts. */
final class ExpressionTypeSemantics {
  private ExpressionTypeSemantics() {}

  static boolean isCharacter(RexNode arg) {
    return arg.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
  }

  static int temporalTypeCode(RelDataType type) {
    SqlTypeName name = type.getSqlTypeName();
    if (name.getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      return 12;
    }
    if (name.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
      return 13;
    }
    switch (name) {
      case DATE:
        return 9;
      case TIME:
        return 10;
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 11;
      default:
        return -1;
    }
  }

  static boolean isInt32(RexNode arg) {
    return List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.NULL)
        .contains(arg.getType().getSqlTypeName());
  }

  static boolean isBuiltinDefinition(
      RexCall call, org.apache.flink.table.functions.BuiltInFunctionDefinition definition) {
    return call.getOperator()
            instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
        && ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction)
                    call.getOperator())
                .getDefinition()
            == definition;
  }
}
