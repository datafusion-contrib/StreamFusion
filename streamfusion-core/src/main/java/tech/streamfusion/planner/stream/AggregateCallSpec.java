package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.types.logical.LogicalType;
import tech.streamfusion.planner.common.GroupAggregateKind;

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
        (kind == GroupAggregateKind.MIN || kind == GroupAggregateKind.MAX)
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
        case GroupAggregateKind.COUNT:
          result = GroupAggregateKind.COUNT_DISTINCT;
          break;
        case GroupAggregateKind.SUM:
          result = GroupAggregateKind.SUM_DISTINCT;
          break;
        case GroupAggregateKind.AVG:
          result = GroupAggregateKind.AVG_DISTINCT;
          break;
        case GroupAggregateKind.SUM_ZERO:
          result = GroupAggregateKind.SUM_ZERO_DISTINCT;
          break;
        case GroupAggregateKind.LISTAGG_APPEND:
          result = GroupAggregateKind.LISTAGG_DISTINCT_APPEND;
          break;
        case GroupAggregateKind.LISTAGG_RETRACT:
          result = GroupAggregateKind.LISTAGG_DISTINCT_RETRACT;
          break;
        case GroupAggregateKind.FIRST_VALUE_APPEND:
        case GroupAggregateKind.FIRST_VALUE_RETRACT:
          result = GroupAggregateKind.FIRST_VALUE_DISTINCT;
          break;
        case GroupAggregateKind.LAST_VALUE_APPEND:
        case GroupAggregateKind.LAST_VALUE_RETRACT:
          result = GroupAggregateKind.LAST_VALUE_DISTINCT;
          break;
        default:
          break;
      }
    }
    if ((result == GroupAggregateKind.MIN || result == GroupAggregateKind.MAX)
        && !retracting
        && runningExtreme)
      result =
          result == GroupAggregateKind.MIN
              ? GroupAggregateKind.MIN_APPEND
              : GroupAggregateKind.MAX_APPEND;
    if (retention > 0
        && (result == GroupAggregateKind.COUNT_DISTINCT
            || result == GroupAggregateKind.SUM_DISTINCT
            || result == GroupAggregateKind.AVG_DISTINCT
            || result == GroupAggregateKind.AVG_DISTINCT_PARTIAL_SUM
            || result == GroupAggregateKind.FIRST_VALUE_DISTINCT
            || result == GroupAggregateKind.LAST_VALUE_DISTINCT
            || result == GroupAggregateKind.SUM_ZERO_DISTINCT
            || result == GroupAggregateKind.COLLECT_DISTINCT
            || result == GroupAggregateKind.ARRAY_AGG_DISTINCT
            || result == GroupAggregateKind.ARRAY_AGG_DISTINCT_IGNORE_NULLS
            || result == GroupAggregateKind.LISTAGG_DISTINCT_APPEND
            || result == GroupAggregateKind.LISTAGG_DISTINCT_RETRACT)) {
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
            ? GroupAggregateKind.JSON_ARRAY_ABSENT_ON_NULL
            : GroupAggregateKind.JSON_ARRAY_NULL_ON_NULL;
      case JSON_OBJECTAGG:
        return (tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON
                ? GroupAggregateKind.JSON_OBJECT_LEGACY_ABSENT_ON_NULL
                : GroupAggregateKind.JSON_OBJECT_ABSENT_ON_NULL)
            + (((org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction) call.getAggregation())
                        .getNullClause()
                    == org.apache.calcite.sql.SqlJsonConstructorNullClause.ABSENT_ON_NULL
                ? 0
                : 1);
      case LISTAGG:
        return retracting ? GroupAggregateKind.LISTAGG_RETRACT : GroupAggregateKind.LISTAGG_APPEND;
      case FIRST_VALUE:
        return retracting
            ? GroupAggregateKind.FIRST_VALUE_RETRACT
            : GroupAggregateKind.FIRST_VALUE_APPEND;
      case LAST_VALUE:
        return retracting
            ? GroupAggregateKind.LAST_VALUE_RETRACT
            : GroupAggregateKind.LAST_VALUE_APPEND;
      case SINGLE_VALUE:
        return GroupAggregateKind.SINGLE_VALUE;
      case SUM0:
        if (call.getArgList().size() != 1) return -1;
        var value = input.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        return isRunningType(value) || value == SqlTypeName.DECIMAL
            ? GroupAggregateKind.SUM_ZERO
            : -1;
      default:
        return scalarKind(call.getAggregation().getKind());
    }
  }

  static int scalarKind(SqlKind kind) {
    switch (kind) {
      case SUM:
        return GroupAggregateKind.SUM;
      case MIN:
        return GroupAggregateKind.MIN;
      case MAX:
        return GroupAggregateKind.MAX;
      case COUNT:
        return GroupAggregateKind.COUNT;
      case AVG:
        return GroupAggregateKind.AVG;
      case SUM0:
        return GroupAggregateKind.SUM_ZERO;
      case SINGLE_VALUE:
        return GroupAggregateKind.SINGLE_VALUE;
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
      return call.isDistinct() ? GroupAggregateKind.COLLECT_DISTINCT : GroupAggregateKind.COLLECT;
    if (tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
      return (call.ignoreNulls()
              ? GroupAggregateKind.ARRAY_AGG_IGNORE_NULLS
              : GroupAggregateKind.ARRAY_AGG)
          + (call.isDistinct() ? 1 : 0);
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
