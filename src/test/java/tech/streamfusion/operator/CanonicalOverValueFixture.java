package tech.streamfusion.operator;

import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public final class CanonicalOverValueFixture {
  private CanonicalOverValueFixture() {}

  public static java.util.stream.Stream<String> countTypes() {
    return java.util.stream.Stream.of(
        "VARBINARY(32)",
        "INTERVAL YEAR TO MONTH",
        "INTERVAL DAY TO SECOND(3)",
        "ARRAY<BIGINT>",
        "ROW<x BIGINT,y STRING>",
        "MAP<STRING,BIGINT>",
        "MULTISET<STRING>",
        "ROW<a ARRAY<BIGINT>,m MAP<STRING,BIGINT>,r ROW<x BIGINT,y STRING>>");
  }

  static String serialized(RowData row, RowType type) {
    try {
      var projectedFields = new java.util.ArrayList<RowType.RowField>();
      var projectedValues = new java.util.ArrayList<Object>();
      for (int index = 0; index < type.getFieldCount(); index++) {
        if (org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
            type.getTypeAt(index))) continue;
        projectedFields.add(type.getFields().get(index));
        projectedValues.add(
            RowData.createFieldGetter(type.getTypeAt(index).copy(true), index).getFieldOrNull(row));
      }
      if (projectedFields.size() != type.getFieldCount()) {
        type = new RowType(projectedFields);
        row =
            org.apache.flink.table.data.GenericRowData.ofKind(
                row.getRowKind(), projectedValues.toArray());
      }
      var bytes = new org.apache.flink.core.memory.DataOutputSerializer(256);
      new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type).serialize(row, bytes);
      return java.util.Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer());
    } catch (java.io.IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  static java.util.List<RowData> physicalRows(java.util.List<RowData> rows, RowType type) {
    var serializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type);
    return rows.stream()
        .map(row -> (RowData) serializer.toBinaryRow(row).copy())
        .collect(java.util.stream.Collectors.toList());
  }

  static int nativeCode(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return 5;
      case SMALLINT:
        return 4;
      case INTEGER:
        return 2;
      case BIGINT:
        return 0;
      case FLOAT:
        return 6;
      case DOUBLE:
        return 1;
      case CHAR:
      case VARCHAR:
        return 3;
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
        var decimal = (DecimalType) type;
        return NativeWindowOperatorCore.decimalCode(decimal.getPrecision(), decimal.getScale());
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
      case ARRAY:
      case ROW:
      case MAP:
      case MULTISET:
        return 0;
      default:
        throw new AssertionError(type);
    }
  }

  public static Object number(LogicalType type, Long value) {
    if (value == null) return null;
    switch (type.getTypeRoot()) {
      case TINYINT:
        return value.byteValue();
      case SMALLINT:
        return value.shortValue();
      case INTEGER:
        return value.intValue();
      case BIGINT:
        return value;
      case FLOAT:
        return value.floatValue();
      case DOUBLE:
        return value.doubleValue();
      case CHAR:
      case VARCHAR:
        return StringData.fromString(value % 2 == 0 ? "λ-雪-" + value : "😀-" + value);
      case DATE:
        return value.intValue();
      case TIME_WITHOUT_TIME_ZONE:
        return (int) Math.floorMod(value, 86400000L);
      case BOOLEAN:
        return value % 2 != 0;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromEpochMillis(value, 123456);
      case DECIMAL:
        var decimal = (DecimalType) type;
        return DecimalData.fromBigDecimal(
            BigDecimal.valueOf(value), decimal.getPrecision(), decimal.getScale());
      case VARBINARY:
        return java.nio.ByteBuffer.allocate(8).putLong(value).array();
      case INTERVAL_YEAR_MONTH:
        return value.intValue();
      case INTERVAL_DAY_TIME:
        return value;
      case ARRAY:
        return new org.apache.flink.table.data.GenericArrayData(new Long[] {value, null, -value});
      case ROW:
        return type.getChildren().get(0) instanceof org.apache.flink.table.types.logical.ArrayType
            ? tech.streamfusion.CanonicalNestedJoinFixture.payload(value)
            : org.apache.flink.table.data.GenericRowData.of(
                value, StringData.fromString("雪-" + value));
      case MAP:
        var values = new java.util.LinkedHashMap<StringData, Long>();
        values.put(StringData.fromString("λ"), value);
        values.put(StringData.fromString("null"), null);
        return new org.apache.flink.table.data.GenericMapData(values);
      case MULTISET:
        var counts = new java.util.LinkedHashMap<StringData, Integer>();
        counts.put(StringData.fromString("雪-" + value), 2);
        counts.put(null, 1);
        return new org.apache.flink.table.data.GenericMapData(counts);
      default:
        throw new AssertionError(type);
    }
  }
}
