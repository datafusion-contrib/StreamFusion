package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkFloatingGroupAggregateSqlHarnessTest {
  private static final DataType INPUT =
      ROW(FIELD("k", INT()), FIELD("id", INT()), FIELD("f", FLOAT()), FIELD("d", DOUBLE()));

  @ParameterizedTest
  @ValueSource(ints = {0, 2, 5})
  void floatReductionsKeepTheirWidthAndPrimitiveExtremaAcrossPartials(int bundle) throws Exception {
    compare(bundle, false, "SELECT k, SUM(f), MIN(f), MAX(f) FROM src GROUP BY k");
    compare(
        bundle,
        false,
        "SELECT SUM(f) FILTER (WHERE k = 2), MIN(f) FILTER (WHERE k = 99),"
            + " MAX(f) FILTER (WHERE id >= 2) FROM src");
    compare(bundle, false, "SELECT SUM(f), MIN(f), MAX(f) FROM src WHERE k = 99");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2, 5})
  void realSqlSpellingRetainsTheHostPlanningFailure(int bundle) {
    NativeFailureParity.run(
            () -> environment(bundle, false), "SELECT SUM(CAST(f AS REAL)) FROM src")
        .assertFailure(
            org.apache.flink.table.api.TableException.class,
            "Type is not supported: REAL",
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.UNPLANNED);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sumsAndRetractingExtremaHandleNaNsZerosNullsAndGroupRecreation(boolean retract)
      throws Exception {
    compare(0, retract, "SELECT k, SUM(f), MIN(f), MAX(f), MIN(d), MAX(d) FROM src GROUP BY k");
    compare(
        0,
        retract,
        "SELECT k, SUM(DISTINCT f), MIN(DISTINCT f), MAX(DISTINCT f),"
            + " SUM(DISTINCT f) FILTER (WHERE id <> 2) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void firstLastKeepArrivalOrderAndRetractBoxedEqualNaNs(boolean retract) throws Exception {
    compare(
        0,
        retract,
        "SELECT k, FIRST_VALUE(f), LAST_VALUE(f), FIRST_VALUE(d), LAST_VALUE(d) FROM src GROUP BY"
            + " k");
    compare(
        0,
        retract,
        "SELECT k, FIRST_VALUE(f) FILTER (WHERE id >= 2),"
            + " LAST_VALUE(d) FILTER (WHERE id <> 3) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2, 5})
  void singleValueRetainsOneFloatOrDoubleIncludingNull(int bundle) throws Exception {
    compare(bundle, false, "SELECT id, SINGLE_VALUE(f), SINGLE_VALUE(d) FROM src GROUP BY id");
    compare(bundle, false, "SELECT SINGLE_VALUE(f), SINGLE_VALUE(d) FROM src WHERE k = 99");
  }

  @ParameterizedTest
  @ValueSource(strings = {"f", "d", "CAST(NULL AS FLOAT)", "CAST(NULL AS DOUBLE)"})
  void singleValueRetainsTheRuntimeCardinalityError(String value) {
    var comparison =
        NativeFailureParity.run(
            () -> environment(0, false), "SELECT SINGLE_VALUE(" + value + ") FROM src");
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element.",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @CsvSource({
    "2,f",
    "2,d",
    "2,CAST(NULL AS FLOAT)",
    "2,CAST(NULL AS DOUBLE)",
    "5,f",
    "5,d",
    "5,CAST(NULL AS FLOAT)",
    "5,CAST(NULL AS DOUBLE)"
  })
  void singleValuePartialsRetainTheRuntimeCardinalityError(int bundle, String value) {
    var comparison =
        NativeFailureParity.run(
            () -> environment(bundle, false), "SELECT SINGLE_VALUE(" + value + ") FROM src");
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element.",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"f", "d", "CAST(NULL AS FLOAT)", "CAST(NULL AS DOUBLE)"})
  void singleValueGlobalMergeRejectsASecondLocalPartial(String value) {
    var comparison =
        NativeFailureParity.run(
            () -> environment(2, false),
            "SELECT MOD(id, 2), SINGLE_VALUE(" + value + ") FROM src GROUP BY MOD(id, 2)");
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element.",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    assertTrue(
        Arrays.stream(comparison.nativeRun().rootCause().getStackTrace())
            .anyMatch(frame -> frame.getMethodName().equals("updateGroupAggregator")),
        comparison::toString);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SUM(DISTINCT f)",
        "AVG(DISTINCT f)"
      })
  void distinctPartialLayoutsPreserveFloatingResults(String aggregate) throws Exception {
    compare(2, false, "SELECT k, " + aggregate + " FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(strings = {"f", "d"})
  void singleValueWithRedundantDistinctUsesThePlannerReducedSplit(String value) throws Exception {
    compare(2, false, "SELECT id, SINGLE_VALUE(DISTINCT " + value + ") FROM src GROUP BY id");
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 5})
  void distinctExtremaUseThePlannerReducedOrdinarySplit(int bundle) throws Exception {
    compare(bundle, false, "SELECT k, MIN(DISTINCT f), MAX(DISTINCT f) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @CsvSource({
    "false,REDUCTIONS", "true,REDUCTIONS", "false,ORDERED", "true,ORDERED",
    "false,SINGLE", "true,SINGLE", "false,TWO_PHASE", "true,TWO_PHASE",
    "false,DISTINCT", "true,DISTINCT", "false,SINGLE_TWO", "true,SINGLE_TWO"
  })
  void floatingStateSurvivesTwoCheckpointRestores(boolean rocks, String shape) {
    boolean retract = !shape.equals("TWO_PHASE") && !shape.equals("SINGLE_TWO");
    int bundle = retract ? 0 : 2;
    String columns;
    switch (shape) {
      case "ORDERED":
        columns = "k, FIRST_VALUE(f), LAST_VALUE(f), FIRST_VALUE(d), LAST_VALUE(d)";
        break;
      case "SINGLE":
      case "SINGLE_TWO":
        columns = "id, SINGLE_VALUE(f), SINGLE_VALUE(d)";
        break;
      case "DISTINCT":
        columns = "k, SUM(DISTINCT f), SUM(DISTINCT f) FILTER (WHERE id <> 2)";
        break;
      default:
        columns = "k, SUM(f), MIN(f), MAX(f), MIN(d), MAX(d)";
    }
    String key = shape.startsWith("SINGLE") ? "id" : "k";
    String sql = "SELECT " + columns + " FROM recovery_input GROUP BY " + key;
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(retract),
                ExternalTypeInfo.<Row>of(INPUT),
                Schema.newBuilder().fromRowDataType(INPUT).build(),
                retract,
                4,
                8)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), bundle),
              () -> configure(recovery.get(), bundle),
              sql);
      assertEquals(null, comparison.host().failure(), comparison::toString);
      assertEquals(null, comparison.nativeRun().failure(), comparison::toString);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertEquals(
          SqlAuditHarness.materialized(comparison.host().rows()),
          SqlAuditHarness.materialized(comparison.nativeRun().rows()),
          comparison::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      if (bundle > 0)
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
    }
  }

  private static void compare(int bundle, boolean retract, String sql) throws Exception {
    Supplier<TableEnvironment> source = () -> environment(bundle, retract);
    TableEnvironment host = source.get();
    TableEnvironment nativeTable = source.get();
    NativePlanner.install(nativeTable);
    assertEquals(
        host.sqlQuery(sql).getResolvedSchema(), nativeTable.sqlQuery(sql).getResolvedSchema());
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    if (bundle > 0) assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    if (bundle == 0) NativeParity.assertOrderedKindedParity(source, sql);
    else NativeParity.assertChangelogParity(source, sql);
  }

  private static TableEnvironment environment(int bundle, boolean retract) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    configure(table, bundle);
    var stream = fromData(env, rows(retract), ExternalTypeInfo.<Row>of(INPUT));
    var schema = Schema.newBuilder().fromRowDataType(INPUT).build();
    table.createTemporaryView(
        "src",
        retract ? table.fromChangelogStream(stream, schema) : table.fromDataStream(stream, schema));
    return table;
  }

  private static TableEnvironment configure(TableEnvironment table, int bundle) {
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", bundle == 0 ? "ONE_PHASE" : "TWO_PHASE");
    if (bundle > 0) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    }
    return table;
  }

  private static List<Row> rows(boolean retract) {
    List<Row> rows = new ArrayList<>();
    Float[][] groups = {
      {null, -0.0f, 0.0f, -0.0f},
      {Float.intBitsToFloat(0xffc00001), 3f, Float.intBitsToFloat(0x7fc00002), -2f},
      {4f, Float.NaN, -3f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY},
      {null, null},
      {16_777_216f, 1f, -16_777_216f, 2f},
      {-0.0f},
      {0.0f, -0.0f},
      {1.25f, 1.25f, -4.5f, 2.75f}
    };
    int id = 0;
    for (int key = 0; key < groups.length; key++) {
      for (Float value : groups[key]) {
        Double widened = value == null ? null : value.doubleValue();
        rows.add(Row.of(key, id++, value, widened));
      }
    }
    if (retract) {
      for (Row row : new ArrayList<>(rows)) {
        Row deletion = Row.copy(row);
        deletion.setKind(RowKind.DELETE);
        if (deletion.getField(2) instanceof Float && ((Float) deletion.getField(2)).isNaN()) {
          deletion.setField(2, Float.intBitsToFloat(0x7fc00077));
          deletion.setField(3, Double.longBitsToDouble(0x7ff8000000000077L));
        }
        rows.add(deletion);
      }
      rows.add(Row.of(0, id++, -0.0f, -0.0d));
      rows.add(Row.of(1, id++, 1.5f, 1.5d));
    }
    return rows;
  }
}
