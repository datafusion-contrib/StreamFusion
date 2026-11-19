package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.table.api.*;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class FlinkMapPayloadRecoveryTest {
  static Stream<Arguments> cases() {
    return FlinkMapPayloadSqlHarnessTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of("inner", "topn", "offset", "firstn")
                                .map(shape -> Arguments.of(type, rocks, shape))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void retainedPayloadsSurviveTwoCheckpoints(String type, boolean rocks, String shape)
      throws Exception {
    var dtype = FlinkMapPayloadSqlHarnessTest.rowType(type);
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                FlinkMapPayloadSqlHarnessTest.rows(type),
                ExternalTypeInfo.<Row>of(dtype),
                Schema.newBuilder()
                    .fromRowDataType(dtype)
                    .columnByExpression("pt", "PROCTIME()")
                    .build(),
                false,
                16,
                32)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> setup =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            table.createTemporaryView("src", table.from("recovery_input"));
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> setup.apply(recovery.uninterrupted()),
              () -> setup.apply(recovery.get()),
              FlinkMapPayloadSqlHarnessTest.sql(shape));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), FlinkMapPayloadSqlHarnessTest.operator(shape));
      SharedFlinkCluster.assertNativeMemoryReleased("map-payload-recovery");
    }
  }

  static Stream<Arguments> nestedMapProducers() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(false, true).map(computedOrder -> Arguments.of(rocks, computedOrder)));
  }

  @ParameterizedTest
  @MethodSource("nestedMapProducers")
  void nestedMapUdfCompositionRespectsExistingBoundary(boolean rocks, boolean computedOrder)
      throws Exception {
    var dtype = FlinkMapPayloadSqlHarnessTest.rowType("map");
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                FlinkMapPayloadSqlHarnessTest.rows("map"),
                ExternalTypeInfo.<Row>of(dtype),
                Schema.newBuilder().fromRowDataType(dtype).build(),
                false,
                16,
                32)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> setup =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            table.createTemporaryView("src", table.from("recovery_input"));
            table.createTemporarySystemFunction(
                "nested_map", new FlinkMapPayloadSqlHarnessTest.NestedStringMap());
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> setup.apply(recovery.uninterrupted()),
              () -> setup.apply(recovery.get()),
              "SELECT id,r,rn FROM (SELECT id,k,r,ROW_NUMBER() OVER (PARTITION BY k ORDER BY "
                  + (computedOrder ? "CARDINALITY(r.m) DESC,id ASC" : "id ASC")
                  + ") AS rn FROM (SELECT id,k,nested_map(id) AS r FROM src)) WHERE rn<=3");
      runs.assertSuccess(
          computedOrder ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      if (computedOrder)
        assertTrue(
            runs.nativeRun().fallbackReasons().stream()
                .anyMatch(reason -> reason.contains("collection value representation")),
            runs::toString);
      recovery.verifyRepeatedRecovery();
      if (!computedOrder)
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
    }
  }

  static Stream<Arguments> updating() {
    return FlinkMapPayloadSqlHarnessTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of("topn", "offset", "inner")
                                .map(shape -> Arguments.of(type, rocks, shape))));
  }

  @ParameterizedTest
  @MethodSource("updating")
  void updatesDeletesAndReinsertionsSurviveTwoRestores(String type, boolean rocks, String shape)
      throws Exception {
    exerciseUpdates(type, rocks, shape, false);
  }

  static Stream<Arguments> entryOrder() {
    return Stream.of(false, true)
        .flatMap(
            rocks -> Stream.of("topn", "offset", "inner").map(shape -> Arguments.of(rocks, shape)));
  }

  @ParameterizedTest
  @MethodSource("entryOrder")
  void reorderedMapRetractionsRespectVerifiedAdmission(boolean rocks, String shape)
      throws Exception {
    exerciseUpdates("map", rocks, shape, true);
  }

  private void exerciseUpdates(String type, boolean rocks, String shape, boolean reverseKeys)
      throws Exception {
    var dtype =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
            DataTypes.FIELD("k", DataTypes.INT().notNull()),
            DataTypes.FIELD("score", DataTypes.BIGINT()),
            DataTypes.FIELD("p", FlinkMapPayloadSqlHarnessTest.payloadType(type)));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 32; i++) rows.add(event(type, i, 0, RowKind.INSERT));
    for (int i = 0; i < 16; i++) {
      rows.add(event(type, i, 0, RowKind.UPDATE_BEFORE));
      rows.add(event(type, i, 100, RowKind.UPDATE_AFTER));
    }
    for (int i = 16; i < 24; i++) rows.add(event(type, i, 0, RowKind.DELETE));
    for (int i = 16; i < 24; i++) rows.add(event(type, i, 200, RowKind.INSERT));
    if (reverseKeys) {
      for (Row row : rows) {
        int id = ((Long) row.getField(0)).intValue();
        int revision = ((Long) row.getField(2)).intValue() - id * 17 % 37;
        boolean reverse = row.getKind() == RowKind.UPDATE_BEFORE || row.getKind() == RowKind.DELETE;
        var map = new LinkedHashMap<String, Integer>();
        for (String key : reverse ? List.of("BB", "Aa") : List.of("Aa", "BB"))
          map.put(key, id + revision + (key.equals("Aa") ? 1 : 2));
        row.setField(3, map);
      }
    }
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                rows,
                ExternalTypeInfo.<Row>of(dtype),
                Schema.newBuilder().fromRowDataType(dtype).primaryKey("k", "id").build(),
                true,
                24,
                56)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> setup =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            table.createTemporaryView("src", table.from("recovery_input"));
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> setup.apply(recovery.uninterrupted()),
              () -> setup.apply(recovery.get()),
              FlinkMapPayloadSqlHarnessTest.sql(shape));
      if (shape.equals("inner")) {
        assertNull(runs.host().failure(), runs::toString);
        assertNull(runs.nativeRun().failure(), runs::toString);
        assertEquals(NativeFailureParity.Route.FALLBACK, runs.nativeRun().route());
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertEquals(
            FlinkMapPayloadSqlHarnessTest.materialized(runs.host().rows()),
            FlinkMapPayloadSqlHarnessTest.materialized(runs.nativeRun().rows()),
            runs::toString);
      } else runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("retained-row representation")),
          runs::toString);
      recovery.verifyRepeatedRecovery();
      System.out.println(
          "[map-payload-updating] type="
              + type
              + " rocks="
              + rocks
              + " shape="
              + shape
              + " plan="
              + runs.nativeRun().plan());
      SharedFlinkCluster.assertNativeMemoryReleased("map-payload-updating-recovery");
    }
  }

  static Row event(String type, int id, int revision, RowKind kind) {
    return Row.ofKind(
        kind,
        (long) id,
        id % 4,
        (long) (id * 17 % 37 + revision),
        FlinkMapPayloadSqlHarnessTest.payload(type, id + revision));
  }
}
