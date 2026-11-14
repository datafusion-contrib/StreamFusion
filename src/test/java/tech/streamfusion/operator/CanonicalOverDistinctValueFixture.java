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
}
