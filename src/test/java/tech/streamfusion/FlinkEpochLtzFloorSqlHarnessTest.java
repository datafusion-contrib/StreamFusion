package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.compat.FlinkCompat;

class FlinkEpochLtzFloorSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> totalEpochProducersConsumeNativeFloor() {
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          List<String> units = new ArrayList<>(List.of("YEAR", "MONTH", "DAY", "HOUR"));
          if (FlinkCompat.supportsSubHourLtzRounding()) {
            units.addAll(List.of("MINUTE", "SECOND", "MILLISECOND"));
          }
          List<String> outputs = new ArrayList<>();
          for (String unit : units) {
            for (String epoch : List.of("TO_TIMESTAMP_LTZ(n,0)", "TO_TIMESTAMP_LTZ(n,3)",
                "TO_TIMESTAMP_LTZ(n+CAST(1 AS " + type + "),3)",
                "TO_TIMESTAMP_LTZ(CAST(n AS BIGINT)*CAST(1000 AS BIGINT),3)",
                "TO_TIMESTAMP_LTZ(ROUND(n,-1),CASE WHEN id<2500 THEN 0 ELSE 3 END)",
                "TO_TIMESTAMP_LTZ(n,CASE WHEN id<2500 THEN CAST(NULL AS INT) ELSE 3 END)")) {
              outputs.add("FLOOR(" + epoch + " TO " + unit + ")");
            }
          }
          outputs.add("YEAR(FLOOR(TO_TIMESTAMP_LTZ(n,3) TO MONTH))");
          outputs.add("FLOOR(TO_TIMESTAMP_LTZ(n,3) TO DAY) IS NULL");
          String sql = "SELECT id," + String.join(",", outputs) + " FROM src";
          for (String zone : List.of("UTC", "Z", "Etc/UTC")) {
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone, sql, true);
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, zone, sql + " WHERE id<0", true);
          }
        }));
      }
    }
    return cases.stream();
  }

  @Test
  void unprovenZonesAndFallibleEpochInputsRetainTheirRoutes() throws Exception {
    for (String zone : List.of("Asia/Shanghai", "GMT+05:30", "America/New_York", "Europe/London")) {
      FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, zone,
          "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(n,3) TO DAY) FROM src", false);
    }
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(n/0,3) TO DAY) FROM src WHERE id<0", false);
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,CASE WHEN id>=0 THEN FLOOR(TO_TIMESTAMP_LTZ(n,3) TO DAY) "
            + "ELSE FLOOR(TO_TIMESTAMP_LTZ(n/0,3) TO DAY) END FROM src", false);
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(n,CASE WHEN id<0 THEN 1 ELSE 3 END) TO DAY) FROM src",
        false);
    FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
        "SELECT id,CEIL(TO_TIMESTAMP_LTZ(n,3) TO DAY) FROM src", false);
  }

  @Test
  void aCatalogFunctionNamedToTimestampLtzKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("n", DataTypes.BIGINT())),
          List.of(Row.of(0L), Row.of(-1L), Row.of((Object) null)));
      table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
      table.createTemporaryFunction("TO_TIMESTAMP_LTZ", new CustomEpoch());
      return table;
    }, "SELECT FLOOR(`default_catalog`.`default_database`.`TO_TIMESTAMP_LTZ`(n,3) TO DAY) FROM src");
  }

  public static class CustomEpoch extends ScalarFunction {
    @DataTypeHint("TIMESTAMP_LTZ(3)")
    public java.time.Instant eval(Long value, Integer precision) {
      return java.time.Instant.ofEpochMilli(-1);
    }
  }
}
