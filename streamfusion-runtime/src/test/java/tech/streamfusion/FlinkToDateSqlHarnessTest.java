package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkToDateSqlHarnessTest {
  @Test
  void overflowingNumericFieldsFailOnBothEngines() throws Exception {
    String sql = "SELECT TO_DATE(s) FROM inputs";
    for (boolean nativeMode : new boolean[] {false, true}) {
      var tables = TextTimeFunctionTestInputs.textRows("2147483648-01-01");
      if (nativeMode) {
        org.junit.jupiter.api.Assertions.assertTrue(
            tech.streamfusion.planner.NativePlanner.explain(tables, sql).contains("NativeCalc"));
        tech.streamfusion.planner.NativePlanner.install(tables);
      }
      org.junit.jupiter.api.Assertions.assertThrows(
          Exception.class,
          () -> {
            try (var rows = tables.executeSql(sql).collect()) {
              while (rows.hasNext()) {
                rows.next();
              }
            }
          });
    }
  }

  @Test
  void parsesPartialDatesAndReturnsNullForInvalidDates() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::dates, "SELECT id, TO_DATE(s) FROM inputs");
  }

  @Test
  void formattedDatesMatchHost() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("2020-01-01", null),
        "SELECT id, TO_DATE(s, 'yyyy-MM-dd') FROM inputs");
  }
}
