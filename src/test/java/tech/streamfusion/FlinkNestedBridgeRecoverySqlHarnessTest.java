package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkNestedBridgeRecoverySqlHarnessTest {
  @ParameterizedTest(name = "{0} {1} {2}")
  @CsvSource({
    "row, primitive-FLOAT, both",
    "map_key, primitive-DOUBLE, both",
    "row_map, date-sql, argument",
    "map_row, time-long, result",
    "map, timestamp-sql, both",
    "map_key, ltz-integer, both",
    "row, yearmonth-integer, both",
    "map, daytime-long, both"
  })
  void allRowKindsAndRepeatedRecoveryRetainNestedBridgeValues(
      String shape, String bridge, String position) throws Exception {
    var type = FlinkNestedBridgeTableFunctionSqlHarnessTest.sourceType(shape, bridge);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 32; i++)
      rows.add(FlinkNestedBridgeTableFunctionSqlHarnessTest.input(shape, bridge, i));
    for (int i = 0; i < 8; i++) {
      var before = FlinkNestedBridgeTableFunctionSqlHarnessTest.input(shape, bridge, i);
      before.setKind(RowKind.UPDATE_BEFORE);
      rows.add(before);
      for (var kind : List.of(RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var changed = FlinkNestedBridgeTableFunctionSqlHarnessTest.input(shape, bridge, i + 34);
        changed.setField(0, i);
        changed.setKind(kind);
        rows.add(changed);
      }
    }
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  16,
                  40)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () ->
                    FlinkNestedBridgeTableFunctionSqlHarnessTest.register(
                        recovery.uninterrupted(), shape, bridge, position),
                () ->
                    FlinkNestedBridgeTableFunctionSqlHarnessTest.register(
                        recovery.get(), shape, bridge, position),
                FlinkNestedBridgeTableFunctionSqlHarnessTest.query(true)
                    .replace("FROM src s", "FROM recovery_input s"));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertEquals(
            NativeParity.multiset(
                FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.host().rows())),
            NativeParity.multiset(
                FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.nativeRun().rows())));
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
        SharedFlinkCluster.assertNativeMemoryReleased("nested-bridge-recovery");
      }
    }
  }
}
