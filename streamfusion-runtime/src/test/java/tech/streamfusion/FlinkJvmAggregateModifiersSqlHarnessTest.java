package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@org.junit.jupiter.api.condition.DisabledIfSystemProperty(
    named = "streamfusion.experimental.jvmGroupAggregates",
    matches = "false")
class FlinkJvmAggregateModifiersSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stateTtlHintOverridesGlobalRetentionAndCacheEligibility(boolean positiveHint)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireStateTtlHint();
    String property = "streamfusion.experimental.jvmAccumulatorCacheEntries";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "3");
      try (var metrics = new RecoveryExecutionMetrics()) {
        NativeParity.assertKindedParity(
            () -> {
              var env = metrics.environment(new Configuration(), 1, 0);
              var table = StreamTableEnvironment.create(env);
              table.createTemporaryView(
                  "src",
                  tech.streamfusion.compat.FlinkTestSources.fromData(
                      env,
                      Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                      Row.of(1, 5L),
                      Row.of(1, 0L),
                      Row.of(1, 2L)));
              register(table);
              table.getConfig().set("table.exec.state.ttl", positiveHint ? "0 s" : "1 h");
              return table;
            },
            "SELECT /*+ STATE_TTL('src' = '"
                + (positiveHint ? "1h" : "0s")
                + "') */ k, test_aggregate(v) FROM src GROUP BY k");
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
        if (positiveHint) assertEquals(0, metrics.counterValue("jvmAccumulatorCacheHits"));
        else assertTrue(metrics.counterValue("jvmAccumulatorCacheHits") > 0);
      }
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "test_aggregate(v) FILTER (WHERE v > 0)",
        "test_aggregate(v) FILTER (WHERE v IS NULL)",
        "test_aggregate(DISTINCT v)",
        "test_aggregate(DISTINCT v) FILTER (WHERE v > 0)",
        "test_aggregate(DISTINCT v), COUNT(DISTINCT v), test_aggregate(v) FILTER (WHERE v > 0)"
      })
  void generatedModifiersPreserveAppendAndRetractingResults(String calls) {
    for (boolean updating : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var comparison =
            NativeFailureParity.run(
                () -> {
                  var env = metrics.environment(new Configuration(), 1, 0);
                  var table = StreamTableEnvironment.create(env);
                  var rows = new ArrayList<Row>();
                  for (int i = 0; i < 48; i++)
                    rows.add(
                        Row.of(i % 4 == 0 ? null : i % 4, i % 8 == 0 ? null : (long) (i % 7 - 3)));
                  if (updating) {
                    for (int i = 0; i < 48; i++)
                      rows.add(
                          Row.ofKind(
                              RowKind.DELETE,
                              i % 4 == 0 ? null : i % 4,
                              i % 8 == 0 ? null : (long) (i % 7 - 3)));
                    rows.add(Row.of(1, 5L));
                    rows.add(Row.of(1, 5L));
                    rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, 1, 5L));
                    rows.add(Row.ofKind(RowKind.UPDATE_AFTER, 1, 7L));
                  }
                  table.createTemporaryView(
                      "src",
                      table.fromChangelogStream(
                          tech.streamfusion.compat.FlinkTestSources.fromData(
                              env,
                              Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                              rows.toArray(new Row[0])),
                          Schema.newBuilder()
                              .column("k", DataTypes.INT())
                              .column("v", DataTypes.BIGINT())
                              .build()));
                  return register(table);
                },
                "SELECT k, " + calls + ", COUNT(*) FROM src GROUP BY k");
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void distinctDataViewsAndFiltersSurviveRetractionsAndTwoRestores(String backend)
      throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 192; i++) rows.add(Row.of(i % 4, i % 7 == 0 ? null : (long) (i % 5)));
    for (int i = 0; i < 192; i++)
      rows.add(Row.ofKind(RowKind.DELETE, i % 4, i % 7 == 0 ? null : (long) (i % 5)));
    for (int i = 0; i < 64; i++) rows.add(Row.of(i % 4, (long) (i % 5)));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("v", DataTypes.BIGINT())
                    .build(),
                true,
                48,
                256)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> register(recovery.uninterrupted()),
              () -> register(recovery.get()),
              "SELECT k, test_aggregate(DISTINCT v) FILTER (WHERE v IS NOT NULL),"
                  + " view_aggregate(DISTINCT v), COUNT(*) FROM recovery_input GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("jvm-aggregate-modifier-recovery");
    }
  }

  private static TableEnvironment register(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.state.ttl", "1 h");
    table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
    table.createTemporarySystemFunction(
        "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
    return table;
  }
}
