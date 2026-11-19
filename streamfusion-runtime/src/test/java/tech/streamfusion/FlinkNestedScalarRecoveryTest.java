package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.function.UnaryOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisabledIfSystemProperty(named = "streamfusion.experimental.nestedScalarUdfs", matches = "false")
class FlinkNestedScalarRecoveryTest {
  @ParameterizedTest
  @CsvSource({
    "hashmap,1,false",
    "hashmap,3,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,1,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,3,false",
    "hashmap,1,true",
    "hashmap,3,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,1,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,3,true"
  })
  void nestedValuesAndMutableCallbacksSurviveRepeatedTaskRecovery(
      String backend, int parallelism, boolean binary) {
    var type =
        ROW(
            FIELD("id", INT()),
            FIELD(
                "r",
                binary
                    ? NestedBinaryFixtures.type()
                    : ROW(FIELD("value", BIGINT()), FIELD("tags", ARRAY(STRING())))));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) {
      Row nested =
          binary
              ? NestedBinaryFixtures.value(i)
              : i % 11 == 0
                  ? null
                  : Row.ofKind(
                      RowKind.values()[i % 4],
                      i % 5 == 0 ? null : (long) i,
                      i % 7 == 0
                          ? null
                          : new String[] {
                            i == 17 ? "é中😀".repeat(4096) : "value-" + i, null, "tail-" + i
                          });
      rows.add(Row.of(i, nested));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                ExternalTypeInfo.<Row>of(type),
                Schema.newBuilder().fromRowDataType(type).build(),
                false,
                12,
                32)
            .withRescaleDeployment(parallelism, null, null)
            .withExecutionMetrics()) {
      UnaryOperator<TableEnvironment> configure =
          table -> {
            recovery.executionEnvironment().getConfig().enableObjectReuse();
            table.getConfig().set("streamfusion.transpose.batchRows", "5");
            if (binary) NestedBinaryFixtures.register(table);
            else {
              table.createTemporarySystemFunction(
                  "nested_identity", new NestedScalarBenchmark.Identity());
              table.createTemporarySystemFunction(
                  "nested_reuse", new FlinkNestedScalarSqlHarnessTest.Reusable());
            }
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT id, nested_identity(r), nested_reuse(r) FROM recovery_input");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(48, runs.host().rows().size());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(runs.nativeRun().plan().contains("RowDataToArrow"), runs.nativeRun().plan());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
      System.out.printf(
          "[nested-recovery-proof] backend=%s parallelism=%d binary=%s observations=%s%n",
          backend, parallelism, binary, recovery.observations());
    }
  }
}
