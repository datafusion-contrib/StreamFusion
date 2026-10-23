package tech.streamfusion.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import tech.streamfusion.arrow.ArrowConversion;

/** Scalar types verified through released Flink converters at JVM function boundaries. */
final class JvmFunctionTypes {
  private JvmFunctionTypes() {}

  static boolean nestedRow(RelDataType row) {
    return row.getFieldList().stream().allMatch(field -> nested(field.getType()));
  }

  static boolean nested(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case ARRAY:
        return nested(type.getComponentType());
      case MAP:
        return nested(type.getKeyType()) && nested(type.getValueType());
      case ROW:
        return nestedRow(type);
      default:
        return scalar(type);
    }
  }

  static boolean scalarRow(RelDataType row) {
    return row.getFieldList().stream().allMatch(field -> scalar(field.getType()));
  }

  static boolean scalar(RelDataType type) {
    if (type.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME
        || type.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) return true;
    switch (type.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case VARBINARY:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case BINARY:
        return ArrowConversion.nestedScalarUdfsEnabled() || ArrowConversion.binaryEltEnabled();
      default:
        return false;
    }
  }
}
