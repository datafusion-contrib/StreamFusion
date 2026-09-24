package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkLtrimSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id FROM inputs WHERE LTRIM(s, ' a\u4e2d\ud83d\ude00') = 'abc'");
  }

  @Test
  void trimsLiteralUnicodeCharacterSetsAndPropagatesNulls() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, LTRIM(s, ' a\u4e2d\ud83d\ude00'), LTRIM(s, ''), LTRIM(s, CAST(NULL AS STRING))"
            + " FROM inputs");
  }

  @Test
  void javaBackedDynamicTrimSetsMatchHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, LTRIM(s, p) FROM inputs");
  }
}
