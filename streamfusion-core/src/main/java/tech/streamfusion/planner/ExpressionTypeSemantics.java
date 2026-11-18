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

  static int rowCalcTypeCode(RelDataType type) {
    return rowCalcTypeCode(type, false);
  }

  static int rowCalcTypeCode(RelDataType type, boolean preserveCollections) {
    if (preserveCollections && SqlTypeFamily.CHARACTER.contains(type))
      return tech.streamfusion.operator.NativeUdf.TYPE_INTERNAL;
    boolean nested;
    switch (type.getSqlTypeName()) {
      case ARRAY:
        nested = nestedFieldSupported(type.getComponentType(), preserveCollections);
        break;
      case MAP:
        nested =
            SqlTypeFamily.CHARACTER.contains(type.getKeyType())
                    && SqlTypeFamily.CHARACTER.contains(type.getValueType())
                || (preserveCollections || !type.getKeyType().isNullable())
                    && (type.getKeyType().getSqlTypeName() != SqlTypeName.BINARY
                        || preserveCollections
                            && (type.getKeyType().isNullable()
                                || tech.streamfusion.compat.FlinkCompat
                                    .supportsNotNullBinaryMapLeaves())
                            && tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled())
                    && (type.getValueType().getSqlTypeName() != SqlTypeName.BINARY
                        || preserveCollections
                            && (type.getValueType().isNullable()
                                || tech.streamfusion.compat.FlinkCompat
                                    .supportsNotNullBinaryMapLeaves())
                            && tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled())
                    && rowCalcTypeCode(type.getKeyType(), preserveCollections) >= 0
                    && rowCalcTypeCode(type.getValueType(), preserveCollections) >= 0;
        break;
      case MULTISET:
        nested =
            (preserveCollections || !type.getComponentType().isNullable())
                && (type.getComponentType().getSqlTypeName() != SqlTypeName.BINARY
                    || preserveCollections
                        && tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled()
                        && (type.getComponentType().isNullable()
                            || tech.streamfusion.compat.FlinkCompat
                                .supportsNotNullBinaryMapLeaves()))
                && rowCalcTypeCode(type.getComponentType(), preserveCollections) >= 0;
        break;
      case STRUCTURED:
        if (!preserveCollections) return -1;
      case ROW:
        nested =
            type.getFieldList().stream()
                .allMatch(
                    field ->
                        !JsonStringIdentity.containsUnpairedSurrogate(field.getName())
                            && nestedFieldSupported(field.getType(), preserveCollections));
        break;
      default:
        nested = false;
        break;
    }
    return nested ? tech.streamfusion.operator.NativeUdf.TYPE_INTERNAL : hostCastTypeCode(type);
  }

  static boolean nestedFieldSupported(RelDataType type, boolean preserveCollections) {
    return (type.getSqlTypeName() != SqlTypeName.BINARY
            || tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled())
        && rowCalcTypeCode(type, preserveCollections) >= 0;
  }

  static boolean containsRowType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case ROW:
        return true;
      case ARRAY:
      case MULTISET:
        return containsRowType(type.getComponentType());
      case MAP:
        return containsRowType(type.getKeyType()) || containsRowType(type.getValueType());
      default:
        return false;
    }
  }

  static boolean hostSupportsConstructorField(RelDataType type) {
    return tech.streamfusion.compat.FlinkCompat.NARROW_CONSTRUCTORS_COMPILE
        || !java.util.Set.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT)
            .contains(type.getSqlTypeName());
  }

  /**
   * The UDF marshalling code for a host-cast operand/result, or -1 for a type the upcall can't
   * carry.
   */
  static int hostCastTypeCode(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.BINARY) {
      return tech.streamfusion.operator.NativeUdf.TYPE_BINARY;
    }
    int temporal = temporalTypeCode(type);
    if (temporal >= 0) {
      return temporal;
    }
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return tech.streamfusion.operator.NativeUdf.decimalType(type.getPrecision(), type.getScale());
    }
    return udfTypeCode(type.getSqlTypeName());
  }

  /**
   * The native UDF marshalling type code for a SQL type (see NativeUdf.TYPE_*), or -1 if
   * unsupported.
   */
  static int udfTypeCode(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return tech.streamfusion.operator.NativeUdf.decimalType(type.getPrecision(), type.getScale());
    }
    return udfTypeCode(type.getSqlTypeName());
  }

  static int udfTypeCode(SqlTypeName type) {
    switch (type) {
      case VARCHAR:
      case CHAR:
        return 0; // TYPE_STRING
      case BIGINT:
        return 1; // TYPE_LONG
      case INTEGER:
        return 2; // TYPE_INT
      case DOUBLE:
        return 3; // TYPE_DOUBLE
      case BOOLEAN:
        return 4; // TYPE_BOOLEAN
      case FLOAT:
      case REAL:
        return 5; // TYPE_FLOAT
      case SMALLINT:
        return 6; // TYPE_SHORT
      case TINYINT:
        return 7; // TYPE_BYTE
      case VARBINARY:
        return tech.streamfusion.operator.NativeUdf.TYPE_BINARY;
      default:
        return -1;
    }
  }
}
