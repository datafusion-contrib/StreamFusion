package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.FlinkTestCapabilities;

class FlinkComputedUnnestSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void computedArraysPreserveNullsRowKindsAndProjection(boolean updating, boolean left) {
    String join = left ? " LEFT JOIN " : " CROSS JOIN ";
    String suffix = left ? " ON TRUE" : "";
    for (String operand : List.of("ARRAY[vs[1], id]", "ARRAY[id, id + 1]")) {
      check(updating, "SELECT id, e FROM t" + join + "UNNEST(" + operand + ") AS u(e)" + suffix);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ARRAY[id > 3, vs[1] IS NULL]",
        "ARRAY[CAST(id AS DECIMAL(20,3)), CAST(vs[1] AS DECIMAL(20,3))]",
        "ARRAY[TO_TIMESTAMP_LTZ(id, 3), TO_TIMESTAMP_LTZ(id + 1, 3)]",
        "ARRAY[CAST(TO_TIMESTAMP_LTZ(id, 3) AS DATE), CAST(TO_TIMESTAMP_LTZ(id + 1, 3) AS DATE)]",
        "ARRAY[CAST(TO_TIMESTAMP_LTZ(id, 3) AS TIME(0)), CAST(TO_TIMESTAMP_LTZ(id + 1, 3) AS"
            + " TIME(0))]"
      })
  void computedScalarArrayTypesRetainOneElementColumn(String operand) {
    check(false, "SELECT e, id, e FROM t CROSS JOIN UNNEST(" + operand + ") AS u(e)");
  }

  @org.junit.jupiter.api.Test
  void castTimeThreeArrayRetainsTheReleasedPlanningAssertion() {
    String query =
        "SELECT e FROM t CROSS JOIN UNNEST(ARRAY["
            + "CAST(TO_TIMESTAMP_LTZ(id, 3) AS TIME(3)),"
            + "CAST(TO_TIMESTAMP_LTZ(id + 1, 3) AS TIME(3))]) AS u(e)";
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var table = environment(false, metrics);
        var scan = nativeRun ? tech.streamfusion.planner.NativePlanner.install(table) : null;
        var error =
            org.junit.jupiter.api.Assertions.assertThrows(
                AssertionError.class, () -> table.sqlQuery(query).explain());
        org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().contains("TIME(3)"));
        org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().contains("TIME(0)"));
        if (scan != null) assertEquals(0, scan.substitutions());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void computedArraysWithPushedConditionAndRepeatedProjection(boolean updating) {
    check(
        updating,
        "SELECT e, id, e FROM t CROSS JOIN UNNEST(ARRAY[vs[1], id]) AS u(e)"
            + " WHERE e > 0 AND e + 1 < 20");
    check(
        updating,
        "SELECT id, e, y FROM t, UNNEST(ARRAY[vs[1], id]) AS u(e),"
            + " UNNEST(ARRAY[e, e + 1]) AS v(y)");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void computedArraysWithOrdinality(boolean left) {
    Assumptions.assumeTrue(
        FlinkTestCapabilities.UNNEST_ORDINALITY, "Flink 1.18 cannot plan UNNEST WITH ORDINALITY");
    check(
        false,
        "SELECT id, e, o FROM t "
            + (left ? "LEFT JOIN " : "CROSS JOIN ")
            + "UNNEST(ARRAY[vs[1], id]) WITH ORDINALITY AS u(e, o)"
            + (left ? " ON TRUE" : ""));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void appendedOperandKeepsNullAndEmptyCollections(boolean left) {
    FlinkTestCapabilities.requireSqlFunction("ARRAY_APPEND");
    check(
        false,
        "SELECT id, e FROM t "
            + (left ? "LEFT JOIN " : "CROSS JOIN ")
            + "UNNEST(ARRAY_APPEND(vs, id)) AS u(e)"
            + (left ? " ON TRUE" : ""));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void computedMapConstructorRetainsTheReleasedRepresentation(boolean ordinality) {
    Assumptions.assumeTrue(
        !ordinality || FlinkTestCapabilities.UNNEST_ORDINALITY,
        "Flink 1.18 cannot plan UNNEST WITH ORDINALITY");
    String sql =
        "SELECT id, mk, mv"
            + (ordinality ? ", o" : "")
            + " FROM t CROSS JOIN"
            + " UNNEST(MAP['z', id, 'a', vs[1], 'm', id + 1])"
            + (ordinality ? " WITH ORDINALITY AS u(mk, mv, o)" : " AS u(mk, mv)");
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(false, metrics), sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      if (ordinality) assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedArrayFieldUsesItsContainingRow(boolean updating) {
    var comparison =
        NativeFailureParity.run(
            () -> nestedEnvironment(updating),
            "SELECT id, e FROM t CROSS JOIN UNNEST(t.nested.values_array) AS u(e)");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void computedArraysFeedStateAcrossTwoRestores(String backend, boolean updating) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 96; i++) {
      int original = updating && i >= 64 ? i - 64 : i;
      rows.add(
          Row.ofKind(
              updating && i >= 64 ? RowKind.DELETE : RowKind.INSERT,
              (long) original,
              collection(original)));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"id", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
                schema(),
                updating,
                24,
                72)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", "false");
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT e, COUNT(*) FROM recovery_input CROSS JOIN"
                  + " UNNEST(ARRAY[vs[1], id]) AS u(e) GROUP BY e");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeUnnest");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("computed-unnest-recovery");
    }
  }

  private static void check(boolean updating, String sql) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(updating, metrics), sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeUnnest");
      assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  private static TableEnvironment environment(boolean updating, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var rows = new ArrayList<Row>();
    for (long id : new long[] {10, 1, 7, 3, 9}) rows.add(Row.of(id, collection((int) id)));
    if (updating) {
      for (long id : new long[] {10, 1, 7, 3, 9}) {
        rows.add(Row.ofKind(RowKind.DELETE, id, collection((int) id)));
      }
    }
    var stream =
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
            rows.toArray(new Row[0]));
    if (updating) {
      table.createTemporaryView(
          "t", table.fromChangelogStream(stream, schema(), ChangelogMode.all()));
    } else {
      table.createTemporaryView("t", stream, schema());
    }
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }

  private static TableEnvironment nestedEnvironment(boolean updating) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var rows = new ArrayList<Row>();
    for (long id : new long[] {10, 1, 7, 3, 9}) {
      rows.add(Row.of(id, id == 3 ? null : Row.of((Object) collection((int) id))));
    }
    if (updating) {
      for (long id : new long[] {10, 1, 7, 3, 9}) {
        rows.add(
            Row.ofKind(RowKind.DELETE, id, id == 3 ? null : Row.of((Object) collection((int) id))));
      }
    }
    var stream =
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "nested"},
                Types.LONG,
                Types.ROW_NAMED(new String[] {"values_array"}, Types.OBJECT_ARRAY(Types.LONG))),
            rows.toArray(new Row[0]));
    var nestedSchema =
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column(
                "nested",
                DataTypes.ROW(DataTypes.FIELD("values_array", DataTypes.ARRAY(DataTypes.BIGINT()))))
            .build();
    if (updating) {
      table.createTemporaryView(
          "t", table.fromChangelogStream(stream, nestedSchema, ChangelogMode.all()));
    } else {
      table.createTemporaryView("t", stream, nestedSchema);
    }
    return table;
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("id", DataTypes.BIGINT())
        .column("vs", DataTypes.ARRAY(DataTypes.BIGINT()))
        .build();
  }

  private static Long[] collection(int id) {
    switch (id % 5) {
      case 0:
        return new Long[] {-1L, 0L};
      case 1:
        return new Long[] {3L, null, 4L};
      case 2:
        return new Long[] {};
      case 3:
        return null;
      default:
        return new Long[] {null};
    }
  }
}
