package tech.streamfusion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkTrimSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"BOTH", "LEADING", "TRAILING"})
  void literalSetsMatchFlinkIncludingUnicodeEmptyAndNull(String direction) throws Exception {
    String[] sets = {"' '", "'ab'", "' ab'", "''", "'\u4e2d\ud83d\ude00'", "CAST(NULL AS STRING)"};
    String sql = "SELECT id";
    for (String set : sets) {
      sql += ", TRIM(" + direction + " " + set + " FROM s)";
    }
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                "   ",
                "  abba  ",
                "abVALUEba",
                "a",
                "b",
                "\t value \n",
                "\u4e2d\ud83d\ude00value\ud83d\ude00\u4e2d",
                "\u00a0x\u00a0"),
        sql + " FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOTH", "LEADING", "TRAILING"})
  void columnTrimSetsMatchFlink(String direction) throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows(" aa ", "bb", null),
        "SELECT TRIM(" + direction + " s FROM s) FROM inputs");
  }
}
