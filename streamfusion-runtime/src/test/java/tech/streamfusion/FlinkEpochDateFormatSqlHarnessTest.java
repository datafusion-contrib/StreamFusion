package tech.streamfusion;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkEpochDateFormatSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> defaultEpochPrecisionKeepsItsReleasedSignature() {
    if (org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18.")) {
      return Stream.of(DynamicTest.dynamicTest("single-argument epoch signature is absent", () -> {
        for (boolean nativeRun : new boolean[] {false, true}) {
          var table = BuiltinFunctionParity.environment(
              DataTypes.ROW(DataTypes.FIELD("n", DataTypes.BIGINT())), List.of(Row.of(1L)));
          if (nativeRun) tech.streamfusion.planner.NativePlanner.install(table);
          org.junit.jupiter.api.Assertions.assertThrows(ValidationException.class,
              () -> table.sqlQuery("SELECT DATE_FORMAT(TO_TIMESTAMP_LTZ(n),'yyyy-MM-dd') FROM src"));
        }
      }));
    }
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT").map(type ->
        DynamicTest.dynamicTest(type, () -> {
          for (boolean nullable : new boolean[] {false, true}) {
            String sql = "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(n),'yyyy-MM-dd HH:mm:ss'),"
                + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n*n),'yyyy-MM-dd') FROM src";
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true);
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true);
          }
        }));
  }

  @ParameterizedTest
  @CsvSource({"TINYINT,true", "TINYINT,false", "SMALLINT,true", "SMALLINT,false",
      "INT,true", "INT,false", "BIGINT,true", "BIGINT,false"})
  void verifiedIntegerEpochsFormatWithNativeWidthsBoundsAndNulls(String type, boolean nullable)
      throws Exception {
    String one = "CAST(1 AS " + type + ")";
    String sql = "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(n,0),'yyyy-MM-dd HH:mm:ss'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n+" + one + ",3),'yyyy-MM-dd''T''HH:mm:ss'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n*n,0),'yyyy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(ROUND(n,-2),3),'yyyy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,CASE WHEN n IS NULL THEN CAST(NULL AS INT) "
        + "WHEN n<CAST(0 AS " + type + ") THEN 0 ELSE 3 END),'yyyy-MM-dd HH:mm:ss'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,0),'yyyy-MM-dd') IS NULL,"
        + "CHAR_LENGTH(DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yyyy-MM-dd')),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yyyy-MM-dd') LIKE '19%',"
        + "CASE WHEN MOD(id,3)=0 THEN DATE_FORMAT(TO_TIMESTAMP_LTZ(n*n,3),'yyyy-MM-dd') "
        + "ELSE CAST(NULL AS STRING) END FROM src";
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true);
    FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true);
  }

  @ParameterizedTest
  @CsvSource({"Etc/UTC", "+00:00", "GMT"})
  void normalizedZeroOffsetAliasesKeepTheSameNativeComposition(String zone) throws Exception {
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, zone,
        "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(n*n,3),'yyyy-MM-dd HH:mm:ss') FROM src", true);
  }

  @Test
  void regionalAndNonzeroZonesKeepGeneratedFormatting() throws Exception {
    for (String zone : List.of("America/New_York", "Europe/London", "+01:00")) {
      String sql = "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yyyy-MM-dd HH:mm:ss') FROM src";
      FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, zone, sql, false);
      FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, zone, sql + " WHERE id<0", false);
    }
  }

  @Test
  void unverifiedProducersAndPatternsRetainGeneratedEvaluation() throws Exception {
    for (String expression : List.of(
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(n/CAST(2 AS BIGINT),3),'yyyy-MM-dd')",
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(n AS TINYINT),3),'yyyy-MM-dd')",
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(n AS DECIMAL(38,0)),3),'yyyy-MM-dd')",
        "DATE_FORMAT(CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(n,3) "
            + "ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END,'yyyy-MM-dd')",
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yyyy-MM-dd[]')",
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yyyy-MM-dd HH:mm:ss.SSSSSSSSS')",
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN 1 ELSE 3 END),'yyyy-MM-dd')")) {
      FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
          "SELECT id," + expression + " FROM src", false);
    }
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(n/CAST(0 AS BIGINT),3),'yyyy-MM-dd') "
            + "FROM src WHERE id<0", false);
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,CASE WHEN id<0 THEN "
            + "DATE_FORMAT(TO_TIMESTAMP_LTZ(n/CAST(0 AS BIGINT),3),'yyyy-MM-dd') "
            + "ELSE DATE_FORMAT(TO_TIMESTAMP_LTZ(n,3),'yyyy-MM-dd') END FROM src", false);
  }
}
