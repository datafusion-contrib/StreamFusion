package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.types.logical.LogicalType;

/** Immutable call semantics shared by relational GROUP BY and released-plan qualification. */
final class AggregateCallSpec {
  private final AggregateCall call;
  private final boolean retracting;
  private final int kind;
  private final int valueColumn;
  private final int valueType;
  private final int auxiliaryColumn;
  private final boolean runningExtreme;

  private AggregateCallSpec(AggregateCall call, RelDataType input, boolean retracting) {
    this.call = call;
    this.retracting = retracting;
    kind = baseKind(call, input, retracting);
    valueColumn = valueColumn(call);
    auxiliaryColumn = auxiliaryColumn(call);
    valueType = physicalValueCode(call, input);
    runningExtreme =
        (kind == 1 || kind == 2)
            && valueColumn >= 0
            && runningExtremeType(input.getFieldList().get(valueColumn).getType().getSqlTypeName());
  }

  static AggregateCallSpec describe(AggregateCall call, RelDataType input, boolean retracting) {
    return new AggregateCallSpec(call, input, retracting);
  }

  AggregateCall call() {
    return call;
  }

  int baseKind() {
    return kind;
  }

  int valueColumn() {
    return valueColumn;
  }

  int valueType() {
    return valueType;
  }

  int filterColumn() {
    return call.filterArg;
  }

  int auxiliaryColumn() {
    return auxiliaryColumn;
  }

  int nativeKind(long retention) {
    int result = kind;
    if (call.isDistinct()) {
      switch (kind) {
        case 3:
          result = 7;
          break;
        case 0:
          result = 9;
          break;
        case 4:
          result = 17;
          break;
        case 22:
          result = 34;
          break;
        case 24:
          result = 32;
          break;
        case 25:
          result = 33;
          break;
        case 12:
        case 15:
          result = 19;
          break;
        case 13:
        case 16:
          result = 20;
          break;
        default:
          break;
      }
    }
    if ((result == 1 || result == 2) && !retracting && runningExtreme)
      result = result == 1 ? 10 : 11;
    if (retention > 0
        && (result == 7
            || result == 9
            || result == 17
            || result == 18
            || result == 19
            || result == 20
            || result == 34
            || result == 49
            || result == 51
            || result == 53
            || result == 32
            || result == 33)) {
      result |= tech.streamfusion.Native.AGGREGATE_DISTINCT_TTL;
      if (!retracting) result |= tech.streamfusion.Native.AGGREGATE_DISTINCT_APPEND;
    }
    return result;
  }

  static int valueColumn(AggregateCall call) {
    if (call.getArgList().isEmpty()) return -1;
    int argument = call.getAggregation().getKind() == SqlKind.JSON_OBJECTAGG ? 1 : 0;
    return call.getArgList().size() <= argument ? -1 : call.getArgList().get(argument);
  }

  /** Released scalar accumulator fields; MapViews and distinct views are separate owners. */
  static int accumulatorWidth(AggregateCall call, boolean retracting) {
    return accumulatorWidth(call.getAggregation().getKind(), retracting);
  }

  static int accumulatorWidth(SqlKind kind, boolean retracting) {
    if (kind == SqlKind.LISTAGG) return retracting ? 1 : 2;
    return kind == SqlKind.AVG || kind == SqlKind.SINGLE_VALUE || kind == SqlKind.SUM && retracting
        ? 2
        : 1;
  }

  private static int baseKind(AggregateCall call, RelDataType input, boolean retracting) {
    int collection = collectionKind(call);
    if (collection >= 0) return collection;
    switch (call.getAggregation().getKind()) {
      case JSON_ARRAYAGG:
        return ((org.apache.calcite.sql.fun.SqlJsonArrayAggAggFunction) call.getAggregation())
                    .getNullClause()
                == org.apache.calcite.sql.SqlJsonConstructorNullClause.ABSENT_ON_NULL
            ? 26
            : 27;
      case JSON_OBJECTAGG:
        return (tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON ? 30 : 28)
            + (((org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction) call.getAggregation())
                        .getNullClause()
                    == org.apache.calcite.sql.SqlJsonConstructorNullClause.ABSENT_ON_NULL
                ? 0
                : 1);
      case LISTAGG:
        return retracting ? 25 : 24;
      case FIRST_VALUE:
        return retracting ? 15 : 12;
      case LAST_VALUE:
        return retracting ? 16 : 13;
      case SINGLE_VALUE:
        return 14;
      case SUM0:
        if (call.getArgList().size() != 1) return -1;
        var value = input.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        return isRunningType(value) || value == SqlTypeName.DECIMAL ? 22 : -1;
      default:
        return scalarKind(call.getAggregation().getKind());
    }
  }

  static int scalarKind(SqlKind kind) {
    switch (kind) {
      case SUM:
        return 0;
      case MIN:
        return 1;
      case MAX:
        return 2;
      case COUNT:
        return 3;
      case AVG:
        return 4;
      case SUM0:
        return 22;
      case SINGLE_VALUE:
        return 14;
      default:
        return -1;
    }
  }

  static int auxiliaryColumn(AggregateCall call) {
    if (call.getArgList().size() != 2) return -1;
    switch (call.getAggregation().getKind()) {
      case JSON_OBJECTAGG:
        return call.getArgList().get(0);
      case LISTAGG:
        return call.getArgList().get(1);
      default:
        return -1;
    }
  }

  private static int physicalValueCode(AggregateCall call, RelDataType input) {
    if (call.getArgList().isEmpty()) return 0;
    var type = input.getFieldList().get(call.getArgList().get(0)).getType();
    int code =
        type.getSqlTypeName() == SqlTypeName.TIMESTAMP
                || type.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            ? 1000 + type.getPrecision()
            : WindowAggregateMatcher.typeCode(type);
    boolean collection = collectionKind(call) >= 0;
    SqlKind kind = call.getAggregation().getKind();
    if (collection
        || kind == SqlKind.MIN
        || kind == SqlKind.MAX
        || kind == SqlKind.FIRST_VALUE
        || kind == SqlKind.LAST_VALUE
        || kind == SqlKind.SINGLE_VALUE) {
      var family = type.getSqlTypeName().getFamily();
      code =
          collection
                  && (family == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_YEAR_MONTH
                      || family == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME)
              ? family == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_YEAR_MONTH ? 2 : 0
              : retainedValueTypeCode(type);
    }
    if (call.isDistinct()
        && kind == SqlKind.COUNT
        && code == 0
        && type.getSqlTypeName() != SqlTypeName.BIGINT) return 100;
    return code;
  }

  /** Canonical typed state requires an explicit code, never the physical COUNT fallback. */
  static int canonicalValueCode(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BIGINT:
        return 0;
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      case CHAR:
      case VARCHAR:
        return 3;
      case SMALLINT:
        return 4;
      case TINYINT:
        return 5;
      case FLOAT:
        return 6;
      case DATE:
        return 8;
      case TIME_WITHOUT_TIME_ZONE:
        return 9;
      case BOOLEAN:
        return 10;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 1000
            + org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type);
      case DECIMAL:
        return 2000
            + 100 * org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type)
            + org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale(type);
      default:
        return -1;
    }
  }

  static boolean isIntegerType(SqlTypeName type) {
    switch (type) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return true;
      default:
        return false;
    }
  }

  static boolean isRunningType(SqlTypeName type) {
    return isIntegerType(type) || type == SqlTypeName.FLOAT || type == SqlTypeName.DOUBLE;
  }

  static boolean runningExtremeType(SqlTypeName type) {
    return isRunningType(type)
        || type == SqlTypeName.DATE
        || type == SqlTypeName.TIME
        || type == SqlTypeName.BOOLEAN;
  }

  static int collectionKind(AggregateCall call) {
    if (call.getAggregation()
        == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT)
      return call.isDistinct() ? 49 : 48;
    if (tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
      return (call.ignoreNulls() ? 52 : 50) + (call.isDistinct() ? 1 : 0);
    return -1;
  }

  static int retainedValueTypeCode(RelDataType type) {
    if (intervalValueType(type.getSqlTypeName())) return 100;
    switch (type.getSqlTypeName()) {
      case BINARY:
      case VARBINARY:
      case ARRAY:
      case MAP:
      case ROW:
      case MULTISET:
        return 100;
      case TIME:
        return 9;
      case BOOLEAN:
        return 10;
      default:
        return WindowAggregateMatcher.typeCode(type);
    }
  }

  static boolean intervalValueType(SqlTypeName type) {
    return type.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_YEAR_MONTH
        || type.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME;
  }
}
