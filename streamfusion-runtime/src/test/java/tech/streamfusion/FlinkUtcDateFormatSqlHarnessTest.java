package tech.streamfusion;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkUtcDateFormatSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> directUtcLocalZonedTimestampsConsumePureNativeFormatting() {
    var cases = new ArrayList<DynamicTest>();
    for (int precision : new int[] {0, 3, 6, 9}) {
      for (String zone : List.of("UTC", "Etc/UTC", "+00:00")) {
        cases.add(DynamicTest.dynamicTest("TIMESTAMP_LTZ(" + precision + ") " + zone, () -> {
          String sql = "SELECT id,DATE_FORMAT(ts,'yyyy-MM-dd HH:mm:ss'),"
              + "DATE_FORMAT(ts,'yy-MM-dd'),DATE_FORMAT(ts,'yyyy-MM-dd''T''HH:mm:ss') FROM src";
          verify(precision, zone, sql, 5003, true);
          verify(precision, zone, sql + " WHERE MOD(id,3)=1", 1668, true);
          verify(precision, zone, sql + " WHERE id<0", 0, true);
        }));
      }
    }
    return cases.stream();
  }

  @TestFactory
  Stream<DynamicTest> nonUtcSessionsKeepTheirReleasedZoneRules() {
    return Stream.of("America/New_York", "Europe/London", "+01:00").map(zone ->
        DynamicTest.dynamicTest(zone, () -> {
          String sql = "SELECT id,DATE_FORMAT(ts,'yyyy-MM-dd HH:mm:ss') FROM src";
          verify(9, zone, sql, 5003, false);
          verify(9, zone, sql + " WHERE id<0", 0, false);
        }));
  }

  @Test
  void unsupportedPatternsAndDerivedProducersKeepGeneratedEvaluation() throws Exception {
    verify(9, "UTC", "SELECT id,DATE_FORMAT(ts,'yyyy-MM-dd[]'),"
        + "DATE_FORMAT(ts,'yyyy-MM-dd HH:mm:ss.SSSSSSSSS'),"
        + "DATE_FORMAT(CAST(ts AS TIMESTAMP_LTZ(3)),'yyyy-MM-dd HH:mm:ss') FROM src", 5003, false);
    verify(9, "UTC", "SELECT id,DATE_FORMAT(CASE WHEN MOD(id,3)=1 THEN ts "
        + "ELSE CAST(NULL AS TIMESTAMP_LTZ(9)) END,'yyyy-MM-dd') FROM src", 5003, false);
  }

  @Test
  void catalogFormattersKeepTheirImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(9))),
          List.of(Row.of(Instant.EPOCH), Row.of((Object) null)));
      table.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
      table.createTemporaryFunction("DATE_FORMAT", new CustomFormatter());
      return table;
    }, "SELECT `default_catalog`.`default_database`.`DATE_FORMAT`(ts,'yyyy-MM-dd') FROM src");
  }

  private static void verify(int precision, String zone, String sql, int count, boolean pure)
      throws Exception {
    FlinkDateFormatExtendedYearSqlHarnessTest.verify(precision, sql, count, pure, true, zone);
  }

  public static class CustomFormatter extends org.apache.flink.table.functions.ScalarFunction {
    public String eval(Instant value, String pattern) {
      return value == null ? "custom:null" : "custom:" + pattern;
    }
  }
}
