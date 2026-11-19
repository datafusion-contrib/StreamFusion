package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUtf16IntermediateSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING 'X' FROM 1 FOR 1)"})
  void consumersKeepSplitSurrogatesDistinctFromReplacementBytes(String value) throws Exception {
    NativeParity.assertParity(
        () -> inputs(2051),
        "SELECT id, " + value + " = '?', " + value + " = 'X?', "
            + "'?' = " + value + ", " + value + " <> '?', '?' <> " + value + ", "
            + value + " LIKE '%?%', REPLACE(" + value + ", '?', 'Z'), "
            + "CASE WHEN " + value + " = '?' THEN 1 ELSE 0 END FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING '' FROM 2)"})
  void fusedEqualityFiltersKeepPositiveMatchesAndNulls(String value) throws Exception {
    NativeParity.assertParity(
        () -> inputs(2051), "SELECT id FROM inputs WHERE '?' = " + value);
    NativeParity.assertParity(
        () -> inputs(2051), "SELECT id FROM inputs WHERE " + value + " <> '?'");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, n, p)", "RPAD(s, n, p)", "OVERLAY(s PLACING p FROM n FOR 1)"})
  void runtimeParametersAndNullsRetainIntermediateValues(String value) throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, " + value + " = '?', REPLACE(" + value + ", '?', 'Z') FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD", "RPAD"})
  void growingPaddingReplacementPreservesUtf16BeforeFinalEncoding(String function) throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("a?", "abc?", "", null),
        "SELECT id, REPLACE(" + function + "(s, 15, 'x?'), '?', 'Z') FROM inputs");
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, REPLACE(" + function + "(s, n, p), '?', 'Z') FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD", "RPAD"})
  void growingPaddingComparisonsPreserveRepeatedAndSplitUtf16Units(String function)
      throws Exception {
    String value = function + "(s, n, p)";
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, "
            + value
            + " = 'xyxyabc', 'abcxyxy' = "
            + value
            + ", "
            + value
            + " <> 'xyxyabc', "
            + value
            + " = '\ud83d\ude00', "
            + value
            + " = '\ud83d\ude00?abc' FROM inputs");
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id FROM inputs WHERE "
            + value
            + " = '"
            + (function.equals("LPAD") ? "xyxyabc" : "abcxyxy")
            + "'");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING 'X' FROM 1 FOR 1)"})
  void filtersObserveTheIntermediateBeforeEncoding(String value) throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("\ud83d\ude00", "?", "X?", "abc", "", null),
        "SELECT id FROM inputs WHERE " + value + " LIKE '%?%'");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING 'X' FROM 1 FOR 1)"})
  void allFilteredBatchesDoNotEvaluateFailingConsumers(String value) throws Exception {
    NativeParity.assertParity(
        () -> inputs(2051),
        "SELECT REPLACE(" + value + ", '?', CAST(1 / (id - id) AS STRING)) "
            + "FROM inputs WHERE id = 99999");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING 'X' FROM 1 FOR 1)"})
  void unselectedBranchesDoNotEvaluateFailingConsumers(String value) throws Exception {
    NativeParity.assertParity(
        () -> inputs(2051),
        "SELECT id, CASE WHEN id < 0 THEN REPLACE(" + value
            + ", '?', CAST(1 / (id - id) AS STRING)) ELSE REPLACE(" + value
            + ", '?', 'Z') END FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING 'X' FROM 1 FOR 1)"})
  void unrepresentedIntermediatesCrossingOperatorsRetainExplicitFallback(String value)
      throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> inputs(12),
        "SELECT " + value + ", COUNT(*) FROM inputs GROUP BY " + value,
        "string identity requires a final projection");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LPAD(s, 1, 'x')", "RPAD(s, 1, 'x')", "OVERLAY(s PLACING 'X' FROM 1 FOR 1)"})
  void standaloneFinalProjectionsRetainTheNativeKernels(String value) throws Exception {
    TableEnvironment tables = inputs(12);
    String sql = "SELECT id, " + value + " FROM inputs";
    String plan = NativePlanner.explain(tables, sql);
    assertTrue(plan.contains("jvmFunctions=[]"), plan);
    NativeParity.assertParity(() -> inputs(2051), sql);
  }

  private static TableEnvironment inputs(int count) {
    String[] values = {"\ud83d\ude00", "?", "X?", "abc", "", null};
    String[] rows = new String[count];
    for (int i = 0; i < count; i++) rows[i] = values[i % values.length];
    return TextTimeFunctionTestInputs.textRows(rows);
  }
}
