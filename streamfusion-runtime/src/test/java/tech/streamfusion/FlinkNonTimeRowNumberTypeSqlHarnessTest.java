package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNonTimeRowNumberTypeSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
    "CAST(-n AS TINYINT)", "CAST(-n AS SMALLINT)", "CAST(-n AS DECIMAL(20,2))",
    "s", "CAST(s AS CHAR(8))", "day_value", "time_value", "flag", "t", "instant_value"
  })
  void ascendingScalarOrdersPreserveEveryRankUpdateAndNullPlacement(String order) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    for (String nulls : new String[] {"FIRST", "LAST"}) {
      String query = "SELECT id, k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY " + order
          + " ASC NULLS " + nulls + ") FROM src";
      java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> setup =
          () -> FlinkOverBasicCountsSqlHarnessTest.environment(32);
      String plan = NativePlanner.explain(setup.get(), query);
      assertTrue(plan.contains("NativeColumnarTopN"), plan);
      NativeParity.assertOrderedKindedParity(setup, query);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "hashmap,STRING", "hashmap,'DECIMAL(20,2)'", "hashmap,DATE",
    "hashmap,TIME(3)", "hashmap,BOOLEAN",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,STRING",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,'DECIMAL(20,2)'",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,DATE",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,TIME(3)",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,BOOLEAN"
  })
  void repeatedRecoveryPreservesTypedKeysAndStableTies(String backend, String type)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    var rows = java.util.stream.IntStream.range(0, 96)
        .mapToObj(i -> org.apache.flink.types.Row.of(
            i % 11 == 0 ? null : i % 3, i % 7 == 0 ? null : (long) (96 - i) % 13))
        .collect(tech.streamfusion.compat.ListCollectors.toList());
    String order;
    switch (type) {
      case "DATE": order = "CAST(TO_TIMESTAMP_LTZ(v * 86400000, 3) AS DATE)"; break;
      case "TIME(3)": order = "CAST(TO_TIMESTAMP_LTZ(v, 3) AS TIME(3))"; break;
      case "BOOLEAN": order = "MOD(v, 2) = 0"; break;
      default: order = "CAST(v AS " + type + ")";
    }
    try (var recovery = new PortableSqlRecovery(
        backend, rows,
        org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
            new String[] {"k", "v"}, org.apache.flink.api.common.typeinfo.Types.INT,
            org.apache.flink.api.common.typeinfo.Types.LONG),
        org.apache.flink.table.api.Schema.newBuilder()
            .column("k", org.apache.flink.table.api.DataTypes.INT())
            .column("v", org.apache.flink.table.api.DataTypes.BIGINT()).build(),
        false, 24, 63).withExecutionMetrics()) {
      var comparison = NativeFailureParity.runRecovery(
          recovery::uninterrupted, recovery,
          "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY " + order
              + " ASC NULLS FIRST) FROM recovery_input");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      SharedFlinkCluster.assertNativeMemoryReleased("typed-non-time-row-number-recovery");
    }
  }
}
