package tech.streamfusion;

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.operator.CanonicalRankScalarFixture;

public final class CanonicalMaterializerTypeFixture {
  private CanonicalMaterializerTypeFixture() {}

  public static Stream<String> shapes() {
    return Stream.of(
        "scalar-tinyint",
        "scalar-int",
        "scalar-double",
        "scalar-wide-decimal",
        "scalar-timestamp-nanos",
        "scalar-varbinary",
        "array",
        "row",
        "map",
        "row-key",
        "nested-row-key");
  }

  public static String sql(String shape) {
    switch (shape) {
      case "row-key":
      case "nested-row-key":
        return "BIGINT";
      case "scalar-int":
        return "INT";
      case "array":
        return "ARRAY<BIGINT>";
      case "row":
        return "ROW<i BIGINT,s STRING>";
      case "map":
        return org.example.canonical.CanonicalFixtureFactory.NestedJoinChecksum.TYPE;
      default:
        return CanonicalRankScalarFixture.sql(shape);
    }
  }

  public static LogicalType type(String shape) {
    switch (shape) {
      case "row-key":
      case "nested-row-key":
        return new BigIntType();
      case "scalar-int":
        return new IntType();
      case "array":
        return new ArrayType(new BigIntType());
      case "row":
        return RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
      case "map":
        return CanonicalNestedJoinFixture.PAYLOAD;
      default:
        return CanonicalRankScalarFixture.type(shape);
    }
  }

  public static Object value(String shape, Long value) {
    if (value == null) return null;
    switch (shape) {
      case "row-key":
      case "nested-row-key":
        return value;
      case "scalar-int":
        return value.intValue();
      case "array":
        return new GenericArrayData(new Long[] {value, null, -value});
      case "row":
        return GenericRowData.of(
            value, org.apache.flink.table.data.StringData.fromString("雪-" + value));
      case "map":
        return CanonicalNestedJoinFixture.payload(value);
      default:
        return CanonicalRankScalarFixture.value(shape, Math.toIntExact(value));
    }
  }

  public static String keySql(String shape) {
    return shape.endsWith("row-key")
        ? tech.streamfusion.operator.CanonicalRowKeyFixture.sql(shape.startsWith("nested"))
        : "BIGINT";
  }

  public static LogicalType keyType(String shape) {
    return shape.endsWith("row-key")
        ? tech.streamfusion.operator.CanonicalRowKeyFixture.type(shape.startsWith("nested"))
        : new BigIntType();
  }

  public static Object key(String shape, long id) {
    return shape.endsWith("row-key")
        ? tech.streamfusion.operator.CanonicalRowKeyFixture.key(
            shape.startsWith("nested"),
            id,
            org.apache.flink.types.RowKind.values()[(int) (id % 4)],
            true)
        : id;
  }

  public static long keyId(String shape, org.apache.flink.table.data.RowData row) {
    return shape.endsWith("row-key")
        ? row.getRow(0, ((RowType) keyType(shape)).getFieldCount()).getLong(0)
        : row.getLong(0);
  }
}
