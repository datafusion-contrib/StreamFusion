package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkRuntimeSqlTrimSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"BOTH", "LEADING", "TRAILING"})
  void runtimeSetsPreserveValuesTypesNullsAndConsumers(String direction) throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT n, TRIM(" + direction + " c FROM s) FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT n, CONCAT(TRIM(" + direction + " c FROM s),'!'), "
            + "TRIM(" + direction + " c FROM s) = '?' FROM src WHERE c <> ''");
  }

  @Test
  void computedOperandsEmptySetsAndLazyBranchesRetainHostEvaluation() throws Exception {
    String trim = "TRIM(BOTH LEFT(c,1) FROM s)";
    var comparison = NativeFailureParity.run(this::environment, "SELECT " + trim + " FROM src");
    if (comparison.host().failure() == null) {
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    } else {
      comparison.assertFailure(ArithmeticException.class, "/ by zero",
          NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    }
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT CASE WHEN c = '' THEN 'skip' ELSE " + trim + " END FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT " + trim + " FROM src WHERE n = 999");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT CASE WHEN n >= 0 THEN TRIM(BOTH c FROM s) "
            + "ELSE CAST(CAST('bad' AS INT) AS STRING) END FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOTH", "LEADING", "TRAILING"})
  void binaryOnlySourcesRetainTheRepresentationFallback(String direction) throws Exception {
    NativeParity.assertFallbackReasonContains(FlinkExtremaBuiltinsSqlHarnessTest::binaryEnvironment,
        "SELECT TRIM(" + direction + " b FROM a) FROM src", "string representation");
  }

  @Test
  void asciiRuntimeInputConsumesNativeBatchesWithoutFallback() throws Exception {
    var rows = new ArrayList<Row>();
    var expected = new ArrayList<Row>();
    for (int i = 0; i < 5003; i++) {
      String value = i % 7 == 0 ? null : "abXYZba";
      String set = i % 11 == 0 ? null : "ab";
      rows.add(Row.of(value, set));
      expected.add(value == null || set == null ? Row.of(null, null, null)
          : Row.of("XYZ", "XYZba", "abXYZ"));
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      var type = ROW(FIELD("s", STRING()), FIELD("c", STRING()));
      table.createTemporaryView("src",
          tech.streamfusion.compat.FlinkTestSources.fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
          Schema.newBuilder().fromRowDataType(type).build());
      var scan = NativePlanner.install(table);
      var actual = new ArrayList<Row>();
      try (var output = table.executeSql("SELECT TRIM(BOTH c FROM s), "
          + "TRIM(LEADING c FROM s), TRIM(TRAILING c FROM s) FROM src").collect()) {
        output.forEachRemaining(actual::add);
      }
      assertEquals(expected, actual);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  private TableEnvironment environment() {
    var samples = List.of(
        Row.of("abXYZba", "ab", 0), Row.of("[+XYZ+]", "[]+", 1),
        Row.of(" abXYZba ", " ab", 12),
        Row.of("\u4e2d\ud83d\ude00XYZ\ud83d\ude00\u4e2d", "\u4e2d\ud83d\ude00", 2),
        Row.of("abc", "", 3), Row.of("", "abc", 4), Row.of("aaa", "a", 5),
        Row.of(null, "ab", 6), Row.of("abc", null, 7),
        Row.of("\t abc\t", "\t ", 8), Row.of("\ud800?", "?", 9),
        Row.of("?abc?", "\ud800", 10), Row.of("a\0ba", "a\0", 11));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 2053; i++) rows.add(Row.copy(samples.get(i % samples.size())));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("c", STRING()), FIELD("n", INT())), rows);
  }
}
