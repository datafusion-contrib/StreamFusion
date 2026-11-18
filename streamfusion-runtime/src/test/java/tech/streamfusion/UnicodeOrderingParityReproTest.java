package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class UnicodeOrderingParityReproTest {
  @Test
  void relationalProjection() throws Exception {
    assertOrderingFallback(UnicodeOrderingParityReproTest::unicode,
        "SELECT id, s < t, s <= t, s > t, s >= t FROM n");
  }

  @Test
  void filterChoosesDifferentRow() throws Exception {
    assertOrderingFallback(UnicodeOrderingParityReproTest::unicode,
        "SELECT id FROM n WHERE s < t");
  }

  @Test
  void caseChoosesDifferentBranch() throws Exception {
    assertOrderingFallback(UnicodeOrderingParityReproTest::unicode,
        "SELECT id, CASE WHEN s < t THEN 1 ELSE 0 END FROM n");
  }

  @Test
  void narrowedVarcharComparison() throws Exception {
    assertOrderingFallback(UnicodeOrderingParityReproTest::unicode,
        "SELECT id, CAST(s AS VARCHAR(2)) < CAST(t AS VARCHAR(2)) FROM n");
  }

  @Test
  void equalityControl() throws Exception {
    NativeParity.assertParity(UnicodeOrderingParityReproTest::unicode,
        "SELECT id, s = t, s <> t FROM n");
  }

  @Test
  void asciiControl() throws Exception {
    assertOrderingFallback(() -> strings("a", "b", false),
        "SELECT id, s < t, s <= t, s > t, s >= t FROM n");
  }

  @Test
  void bmpControl() throws Exception {
    assertOrderingFallback(() -> strings("\u4E2D", "\uE000", false),
        "SELECT id, s < t, s <= t, s > t, s >= t FROM n");
  }

  @Test
  void serializedInputControl() throws Exception {
    assertOrderingFallback(() -> strings("\uE000", "\uD83D\uDE00", true),
        "SELECT id, s < t, s <= t, s > t, s >= t FROM n");
  }

  @Test
  void topNControl() throws Exception {
    NativeParity.assertChangelogParity(
        UnicodeOrderingParityReproTest::unicode,
        "SELECT id, rn FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY s, id) rn FROM n) WHERE rn <="
            + " 2");
  }

  private static void assertOrderingFallback(
      java.util.function.Supplier<TableEnvironment> input, String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(input, sql, "string ordering depends");
  }

  private static TableEnvironment unicode() {
    return strings("\uE000", "\uD83D\uDE00", false);
  }

  private static TableEnvironment strings(String first, String second, boolean serialized) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    if (serialized) env.disableOperatorChaining();
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "s", "t"}, Types.INT, Types.STRING, Types.STRING),
            Row.of(1, first, second),
            Row.of(2, second, first)));
    return table;
  }
}
