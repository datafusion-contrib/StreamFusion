package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkUnnestRowOrdinalitySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void oneNullableFieldRetainsNullRowsAndOriginalOrdinality(boolean left) {
    requireOrdinality();
    String query =
        "SELECT k, a, o FROM t "
            + (left ? "LEFT JOIN " : "CROSS JOIN ")
            + "UNNEST(rs) WITH ORDINALITY AS u(a,o)"
            + (left ? " ON TRUE" : "");
    var comparison =
        NativeFailureParity.run(FlinkUnnestRowOrdinalitySqlHarnessTest::singleField, query);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"k, a, b, o", "k, a", "COUNT(*)"})
  void nullableWideRowsKeepReleasedSerializerFailures(String projection) {
    requireOrdinality();
    String query =
        "SELECT " + projection + " FROM t CROSS JOIN UNNEST(rs) WITH ORDINALITY AS u(a,b,o)";
    try (var host = new RecoveryExecutionMetrics();
        var candidate = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () ->
                  FlinkUnnestSqlHarnessTest.rowArrayEnvironment(
                      host.environment(new org.apache.flink.configuration.Configuration(), 1, 0)),
              () ->
                  FlinkUnnestSqlHarnessTest.rowArrayEnvironment(
                      candidate.environment(
                          new org.apache.flink.configuration.Configuration(), 1, 0)),
              query);
      comparison.assertFailure(
          RuntimeException.class,
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.FALLBACK);
      assertEquals(
          comparison.host().rootCause().getMessage(),
          comparison.nativeRun().rootCause().getMessage());
    }
  }

  private static void requireOrdinality() {
    Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.UNNEST_ORDINALITY,
        "Flink 1.18 cannot plan UNNEST WITH ORDINALITY");
  }

  private static TableEnvironment singleField() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"k", "rs"},
                Types.LONG,
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"a"}, Types.LONG))),
            Row.of(1L, new Row[] {Row.of(7L), null, Row.of(9L)}),
            Row.of(2L, new Row[] {}),
            Row.of(3L, null)));
    return table;
  }
}
