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
        "map");
  }

  public static String sql(String shape) {
    switch (shape) {
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
}
