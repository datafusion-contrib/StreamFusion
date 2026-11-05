package tech.streamfusion;

import java.math.BigInteger;
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
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDecimalEpochDateFormatSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void verifiedDecimalEpochsFormatWithReleasedWrappingBoundsAndNulls(int precision, int scale)
      throws Exception {
    String sql = "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d,0),'yyyy-MM-dd HH:mm:ss'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(e,3),'yy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(-d,3),'yyyy-MM-dd''T''HH:mm:ss'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(ABS(e),0),'yyyy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(-(-d),0),'yyyy-MM-dd HH:mm:ss'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(-ABS(d),3),'yyyy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(ABS(-ABS(e)),0),'yyyy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(-d AS DECIMAL(38," + scale + ")),3),'yyyy-MM-dd'),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(d,0),'yyyy-MM-dd') IS NULL,"
        + "CHAR_LENGTH(DATE_FORMAT(TO_TIMESTAMP_LTZ(d,3),'yyyy-MM-dd')),"
        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(d,3),'yyyy-MM-dd') LIKE '19%',"
        + "CASE WHEN MOD(id,3)=0 THEN DATE_FORMAT(TO_TIMESTAMP_LTZ(ABS(e),3),'yyyy-MM-dd') "
        + "ELSE CAST(NULL AS STRING) END FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        verify(precision, scale, nullable, legacy, "UTC", sql, true);
        verify(precision, scale, nullable, legacy, "UTC", sql + " WHERE id<0", true);
      }
    }
  }

  @TestFactory
  Stream<DynamicTest> defaultDecimalEpochPrecisionKeepsItsReleasedSignature() {
    if (org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18.")) {
      return Stream.of(DynamicTest.dynamicTest("single-argument epoch signature is absent", () -> {
        for (boolean nativeRun : new boolean[] {false, true}) {
          var table = BuiltinFunctionParity.environment(
              DataTypes.ROW(DataTypes.FIELD("d", DataTypes.DECIMAL(18, 3))),
              List.of(Row.of(new java.math.BigDecimal("1.001"))));
          if (nativeRun) tech.streamfusion.planner.NativePlanner.install(table);
          org.junit.jupiter.api.Assertions.assertThrows(ValidationException.class,
              () -> table.sqlQuery("SELECT DATE_FORMAT(TO_TIMESTAMP_LTZ(d),'yyyy-MM-dd') FROM src"));
        }
      }));
    }
    return Stream.of(new int[] {18, 3}, new int[] {19, 0}, new int[] {38, 18})
        .map(type -> DynamicTest.dynamicTest("DECIMAL(" + type[0] + "," + type[1] + ")", () -> {
          String sql = "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d),'yyyy-MM-dd HH:mm:ss'),"
              + "DATE_FORMAT(TO_TIMESTAMP_LTZ(-d),'yyyy-MM-dd'),"
              + "DATE_FORMAT(TO_TIMESTAMP_LTZ(ABS(e)),'yy-MM-dd') FROM src";
          for (boolean nullable : new boolean[] {false, true}) {
            verify(type[0], type[1], nullable, false, "UTC", sql, true);
            verify(type[0], type[1], nullable, false, "UTC", sql + " WHERE id<0", true);
          }
        }));
  }

  @ParameterizedTest
  @ValueSource(strings = {"Etc/UTC", "+00:00", "GMT"})
  void normalizedZeroOffsetAliasesKeepNativeDecimalComposition(String zone) throws Exception {
    verify(38, 0, true, false, zone,
        "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d,0),'yyyy-MM-dd HH:mm:ss') FROM src", true);
  }

  @Test
  void regionalAndNonzeroZonesKeepGeneratedFormatting() throws Exception {
    for (String zone : List.of("America/New_York", "Europe/London", "+01:00")) {
      String sql = "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d,3),'yyyy-MM-dd HH:mm:ss') FROM src";
      verify(38, 0, true, false, zone, sql, false);
      verify(38, 0, true, false, zone, sql + " WHERE id<0", false);
    }
  }

  @Test
  void unverifiedDecimalProducersAndPatternsKeepGeneratedEvaluation() throws Exception {
    for (String epoch : List.of("TO_TIMESTAMP_LTZ(d+d,3)", "TO_TIMESTAMP_LTZ(ABS(d+d),3)",
        "TO_TIMESTAMP_LTZ(CAST(d AS DECIMAL(18,0)),3)",
        "TO_TIMESTAMP_LTZ(d,CASE WHEN id<1000 THEN 0 ELSE 3 END)")) {
      verify(18, 3, true, false, "UTC",
          "SELECT id,DATE_FORMAT(" + epoch + ",'yyyy-MM-dd') FROM src", false);
    }
    for (String pattern : List.of("yyyy-MM-dd[]", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")) {
      verify(18, 3, true, false, "UTC",
          "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d,3),'" + pattern + "') FROM src", false);
    }
    verify(18, 3, true, false, "UTC",
        "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d,3),"
            + "CASE WHEN MOD(id,3)=0 THEN 'yyyy-MM-dd' ELSE 'yy-MM-dd' END) FROM src", false);
    verify(18, 3, true, false, "UTC",
        "SELECT id,DATE_FORMAT(TO_TIMESTAMP_LTZ(d/(id-id),3),'yyyy-MM-dd') FROM src WHERE id<0",
        false);
    verify(18, 3, true, false, "UTC",
        "SELECT id,CASE WHEN id>=0 THEN DATE_FORMAT(TO_TIMESTAMP_LTZ(d,0),'yyyy-MM-dd') "
            + "ELSE DATE_FORMAT(TO_TIMESTAMP_LTZ(d/(id-id),3),'yyyy-MM-dd') END FROM src", false);
  }

  private static void verify(int precision, int scale, boolean nullable, boolean legacy,
      String zone, String sql, boolean pure) throws Exception {
    var values = new java.util.ArrayList<>(tech.streamfusion.planner
        .IntegralExpressionBenchmarkFunctions.decimalEpochCoefficients(precision, scale));
    var random = new java.util.Random(20261105L + precision * 41L + scale);
    var limit = BigInteger.TEN.pow(precision);
    for (int i = 0; i < 64; i++) {
      var value = new BigInteger(128, random).mod(limit);
      values.add(i % 2 == 0 ? value : value.negate());
    }
    FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
        sql, pure, values.toArray(new BigInteger[0]), zone);
  }
}
