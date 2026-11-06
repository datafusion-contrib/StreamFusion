package tech.streamfusion;

import java.math.BigInteger;
import java.util.ArrayList;
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
import tech.streamfusion.compat.FlinkCompat;

class FlinkDecimalEpochFloorSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void verifiedDecimalEpochsConsumeExactCalendarAndFixedUnitFloor(int precision, int scale)
      throws Exception {
    var units = new ArrayList<>(List.of("YEAR", "MONTH", "DAY", "HOUR"));
    if (FlinkCompat.supportsSubHourLtzRounding()) {
      units.addAll(List.of("MINUTE", "SECOND", "MILLISECOND"));
    }
    var outputs = new ArrayList<String>();
    for (String unit : units) {
      for (String epoch : List.of("TO_TIMESTAMP_LTZ(d,0)", "TO_TIMESTAMP_LTZ(e,3)",
          "TO_TIMESTAMP_LTZ(-d,3)", "TO_TIMESTAMP_LTZ(ABS(e),0)",
          "TO_TIMESTAMP_LTZ(-ABS(d),3)",
          "TO_TIMESTAMP_LTZ(CAST(-d AS DECIMAL(38," + scale + ")),3)")) {
        outputs.add("FLOOR(" + epoch + " TO " + unit + ")");
      }
    }
    outputs.add("YEAR(FLOOR(TO_TIMESTAMP_LTZ(d,3) TO MONTH))");
    outputs.add("FLOOR(TO_TIMESTAMP_LTZ(e,0) TO DAY) IS NULL");
    outputs.add("CASE WHEN MOD(id,3)=0 THEN FLOOR(TO_TIMESTAMP_LTZ(-d,3) TO HOUR) "
        + "ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END");
    String sql = "SELECT id," + String.join(",", outputs) + " FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      verify(precision, scale, nullable, "UTC", sql, true);
      verify(precision, scale, nullable, "UTC", sql + " WHERE id<0", true);
    }
  }

  @TestFactory
  Stream<DynamicTest> defaultDecimalPrecisionRetainsItsReleasedSignature() {
    if (org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18.")) {
      return Stream.of(DynamicTest.dynamicTest("single-argument epoch signature is absent", () -> {
        for (boolean nativeRun : new boolean[] {false, true}) {
          var table = environment();
          if (nativeRun) tech.streamfusion.planner.NativePlanner.install(table);
          org.junit.jupiter.api.Assertions.assertThrows(ValidationException.class,
              () -> table.sqlQuery("SELECT FLOOR(TO_TIMESTAMP_LTZ(d) TO DAY) FROM src"));
        }
      }));
    }
    return Stream.of(new int[] {18, 3}, new int[] {19, 0}, new int[] {38, 18})
        .map(type -> DynamicTest.dynamicTest("DECIMAL(" + type[0] + "," + type[1] + ")", () -> {
          String sql = "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(d) TO YEAR),"
              + "FLOOR(TO_TIMESTAMP_LTZ(-ABS(e)) TO DAY) FROM src";
          verify(type[0], type[1], true, "UTC", sql, true);
          verify(type[0], type[1], true, "UTC", sql + " WHERE id<0", true);
        }));
  }

  @ParameterizedTest
  @ValueSource(strings = {"Z", "Etc/UTC", "GMT"})
  void normalizedZeroOffsetAliasesKeepNativeFloor(String zone) throws Exception {
    verify(38, 0, true, zone,
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(d,0) TO MONTH),"
            + "FLOOR(TO_TIMESTAMP_LTZ(e,3) TO HOUR) FROM src", true);
  }

  @Test
  void unverifiedZonesProducersAndDecimalCeilKeepGeneratedEvaluation() throws Exception {
    for (String zone : List.of("America/New_York", "Europe/London", "GMT+05:30")) {
      verify(38, 0, true, zone,
          "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(d,3) TO DAY) FROM src", false);
    }
    for (String sql : List.of(
        "SELECT id,CEIL(TO_TIMESTAMP_LTZ(d,3) TO MONTH) FROM src",
        "SELECT id,CEIL(TO_TIMESTAMP_LTZ(d,0) TO DAY) FROM src",
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(d+d,3) TO DAY) FROM src",
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(CAST(d AS DECIMAL(18,0)),3) TO MONTH) FROM src",
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(d,CASE WHEN id<1000 THEN 0 ELSE 3 END) TO DAY) FROM src",
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(d/(id-id),3) TO DAY) FROM src WHERE id<0",
        "SELECT id,CASE WHEN id>=0 THEN FLOOR(TO_TIMESTAMP_LTZ(d,3) TO DAY) "
            + "ELSE FLOOR(TO_TIMESTAMP_LTZ(d/(id-id),3) TO DAY) END FROM src")) {
      verify(18, 3, true, "UTC", sql, false);
    }
  }

  @Test
  void unsupportedReleasedSubHourFormsKeepTheirCodeGenerationFailure() throws Exception {
    if (FlinkCompat.supportsSubHourLtzRounding()) return;
    for (String unit : List.of("MINUTE", "SECOND", "MILLISECOND")) {
      NativeFailureParity.run(FlinkDecimalEpochFloorSqlHarnessTest::environment,
          "SELECT FLOOR(TO_TIMESTAMP_LTZ(d,3) TO " + unit + ") FROM src")
          .assertFailure(org.codehaus.commons.compiler.CompileException.class,
              "No applicable constructor/method", NativeFailureParity.Phase.INITIALIZATION,
              NativeFailureParity.Route.FALLBACK);
    }
  }

  private static org.apache.flink.table.api.TableEnvironment environment() {
    var table = BuiltinFunctionParity.environment(
        DataTypes.ROW(DataTypes.FIELD("d", DataTypes.DECIMAL(18, 3))),
        List.of(Row.of(new java.math.BigDecimal("-1.001")), Row.of((Object) null)));
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    return table;
  }

  private static void verify(int precision, int scale, boolean nullable, String zone,
      String sql, boolean pure) throws Exception {
    var values = new ArrayList<>(tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions
        .decimalEpochCoefficients(precision, scale));
    var limit = BigInteger.TEN.pow(precision);
    var factor = BigInteger.TEN.pow(scale);
    for (long epoch : new long[] {-62135596800L, -12219292800L, -12219292800000L,
        -86401, -86400, -86399, -3601, -3600, -3599, -61, -60, -59, -1, 0, 1}) {
      for (int fraction : new int[] {-1, 0, 1}) {
        var value = BigInteger.valueOf(epoch).multiply(factor).add(BigInteger.valueOf(fraction));
        if (value.abs().compareTo(limit) < 0) values.add(value);
      }
    }
    FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, false,
        sql, pure, values.toArray(new BigInteger[0]), zone);
  }
}
