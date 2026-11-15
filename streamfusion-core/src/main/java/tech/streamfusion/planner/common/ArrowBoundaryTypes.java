package tech.streamfusion.planner.common;

import org.apache.calcite.rel.type.RelDataType;

/** SQL types carried by the shared row/Arrow boundary, independent of plan mode. */
public final class ArrowBoundaryTypes {
  private ArrowBoundaryTypes() {}

  /**
   * Whether every input column has a type the Arrow boundary can carry. Nested ARRAY/MAP/ROW values
   * are admitted recursively because the boundary preserves their complete binary representation.
   */
  public static boolean convertibleRow(RelDataType inputType) {
    return convertibleRow(inputType, false);
  }

  public static boolean convertibleRow(RelDataType inputType, boolean allowStructured) {
    return inputType.getFieldList().stream()
        .allMatch(f -> isConvertibleType(f.getType(), allowStructured));
  }

  private static boolean isConvertibleType(RelDataType type, boolean allowStructured) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DATE:
      case TIME:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case DECIMAL:
        return true;
      case ARRAY:
      case MULTISET:
        return isConvertibleType(type.getComponentType(), allowStructured);
      case MAP:
        return isConvertibleType(type.getKeyType(), allowStructured)
            && isConvertibleType(type.getValueType(), allowStructured);
      case STRUCTURED:
        if (!allowStructured) return false;
      case ROW:
        return convertibleRow(type, allowStructured);
      default:
        return false;
    }
  }
}
