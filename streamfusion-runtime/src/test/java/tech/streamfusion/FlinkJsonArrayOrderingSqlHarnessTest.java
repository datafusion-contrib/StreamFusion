package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkJsonArrayOrderingSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "ASC,false",
    "DESC,false",
    "WITHIN_GROUP,false",
    "ASC,true",
    "DESC,true",
    "WITHIN_GROUP,true"
  })
  void orderingClausesRetainReleasedArrivalOrderAndRetractionSemantics(
      String clause, boolean updating) {
    List<Row> rows =
        new ArrayList<>(
            List.of(row("10", "z"), row("1", "a"), row("7", "m"), row("8", "a"), row("3", null)));
    if (updating) {
      var removed = row("1", "a");
      removed.setKind(RowKind.DELETE);
      rows.add(removed);
      rows.add(row("0", "y"));
    }
    for (String policy : List.of("", " NULL ON NULL", " ABSENT ON NULL")) {
      String aggregate =
          clause.equals("WITHIN_GROUP")
              ? "JSON_ARRAYAGG(v" + policy + ") WITHIN GROUP (ORDER BY json_key DESC)"
              : "JSON_ARRAYAGG(v ORDER BY json_key " + clause + policy + ")";
      var comparison =
          NativeFailureParity.run(
              () ->
                  FlinkStringAggregateHostContractTest.environment(
                      updating, rows.toArray(new Row[0])),
              "SELECT " + aggregate + " FROM src");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"));
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
      String expected = updating ? "[\"z\",\"m\",\"a\"" : "[\"z\",\"a\",\"m\",\"a\"";
      if (policy.equals(" NULL ON NULL")) expected += ",null";
      if (updating) expected += ",\"y\"";
      expected += "]";
      var hostRows = comparison.host().rows();
      assertEquals(expected, hostRows.get(hostRows.size() - 1).get(1));
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "JSON_ARRAYAGG(v ORDER BY json_key ASC NULLS FIRST NULL ON NULL)",
        "JSON_ARRAYAGG(v NULL ON NULL) WITHIN GROUP (ORDER BY json_key DESC NULLS LAST, v ASC)"
      })
  void nullableAndMultipleSortKeysStillRetainArrivalOrder(String aggregate) {
    var comparison =
        NativeFailureParity.run(
            () ->
                FlinkStringAggregateHostContractTest.environment(
                    false, row(null, "z"), row("0", "a"), row(null, null), row("0", "m")),
            "SELECT " + aggregate + " FROM src");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    var rows = comparison.host().rows();
    assertEquals("[\"z\",\"a\",null,\"m\"]", rows.get(rows.size() - 1).get(1));
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void orderingMetadataKeepsArrayStateAcrossTwoRestores(String backend, boolean updating)
      throws Exception {
    for (boolean withinGroup : new boolean[] {false, true}) {
      var rows = new ArrayList<Row>();
      for (int i = 0; i < 96; i++) {
        int original = updating && i >= 64 ? i - 64 : i;
        rows.add(
            Row.ofKind(
                updating && i >= 64 ? RowKind.DELETE : RowKind.INSERT,
                original % 3,
                original % 7 == 0 ? null : "é🚀" + original % 5,
                95L - original));
      }
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                      new String[] {"k", "v", "sort_id"},
                      org.apache.flink.api.common.typeinfo.Types.INT,
                      org.apache.flink.api.common.typeinfo.Types.STRING,
                      org.apache.flink.api.common.typeinfo.Types.LONG),
                  org.apache.flink.table.api.Schema.newBuilder()
                      .column("k", org.apache.flink.table.api.DataTypes.INT())
                      .column("v", org.apache.flink.table.api.DataTypes.STRING())
                      .column("sort_id", org.apache.flink.table.api.DataTypes.BIGINT())
                      .build(),
                  updating,
                  24,
                  72)
              .withExecutionMetrics()) {
        java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure =
            table -> {
              table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
              table.getConfig().set("table.exec.mini-batch.enabled", "false");
              table.getConfig().set("streamfusion.transpose.batchRows", "13");
              return table;
            };
        String aggregate =
            withinGroup
                ? "JSON_ARRAYAGG(v NULL ON NULL) WITHIN GROUP (ORDER BY sort_id DESC)"
                : "JSON_ARRAYAGG(v ORDER BY sort_id ASC NULL ON NULL)";
        var comparison =
            NativeFailureParity.runRecovery(
                () -> configure.apply(recovery.uninterrupted()),
                () -> configure.apply(recovery.get()),
                "SELECT k, " + aggregate + ", COUNT(*) FROM recovery_input GROUP BY k");
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarGroupAggregate");
        SharedFlinkCluster.assertNativeMemoryReleased("JSON-array-ordering-recovery");
      }
    }
  }

  private static Row row(String key, String value) {
    return Row.of(value, ",", key, 1L, true);
  }
}
