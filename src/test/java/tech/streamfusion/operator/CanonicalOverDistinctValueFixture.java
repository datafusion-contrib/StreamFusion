package tech.streamfusion.operator;

import java.util.stream.Stream;

public final class CanonicalOverDistinctValueFixture {
  private CanonicalOverDistinctValueFixture() {}

  public static Stream<String> countTypes() {
    return Stream.of(
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "BOOLEAN",
        "FLOAT",
        "DOUBLE",
        "CHAR(32)",
        "VARCHAR(100)",
        "DECIMAL(18,4)",
        "DECIMAL(38,7)",
        "DATE",
        "TIME(3)",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)");
  }

  public static boolean integral(String sql) {
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT")
        .anyMatch(type -> sql.equals(type) || sql.equals(type + " NOT NULL"));
  }

  public static boolean arithmetic(String sql) {
    return integral(sql)
        || sql.startsWith("DECIMAL(")
        || Stream.of("FLOAT", "DOUBLE")
            .anyMatch(type -> sql.equals(type) || sql.equals(type + " NOT NULL"));
  }

  public static Object number(org.apache.flink.table.types.logical.LogicalType type, Long value) {
    if (value == null) return null;
    if (type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT)) {
      if (value == 7) return -0.0f;
      if (value == 8) return 0.0f;
      if (value == 9) return Float.intBitsToFloat(0x7fc00011);
      if (value == 10) return Float.intBitsToFloat(0xffc00022);
      if (value == 11) return Float.POSITIVE_INFINITY;
      if (value == 3) return Float.NEGATIVE_INFINITY;
      if (value == 4) return Float.MIN_VALUE;
    }
    if (type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE)) {
      if (value == 7) return -0.0d;
      if (value == 8) return 0.0d;
      if (value == 9) return Double.longBitsToDouble(0x7ff8000000000011L);
      if (value == 10) return Double.longBitsToDouble(0xfff8000000000022L);
      if (value == 11) return Double.POSITIVE_INFINITY;
      if (value == 3) return Double.NEGATIVE_INFINITY;
      if (value == 4) return Double.MIN_VALUE;
    }
    return CanonicalOverValueFixture.number(type, value);
  }
}
