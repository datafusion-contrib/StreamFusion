package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkRtrimSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id FROM inputs WHERE RTRIM(s, ' a\u4e2d\ud83d\ude00') = 'abc'");
  }

  @Test
  void trimsLiteralUnicodeCharacterSetsAndPropagatesNulls() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, RTRIM(s, ' a\u4e2d\ud83d\ude00'), RTRIM(s, ''), RTRIM(s, CAST(NULL AS STRING))"
            + " FROM inputs");
  }

  @Test
  void javaBackedDynamicTrimSetsMatchHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, RTRIM(s, p) FROM inputs");
  }
}
