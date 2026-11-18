package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@DisabledIfSystemProperty(named = "streamfusion.experimental.nestedScalarUdfs", matches = "false")
class FlinkMapScalarUdfRecoveryTest {
  static Stream<Arguments> cases() {
    return Stream.of("map", "int_map", "map_map", "decimal_timestamp")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(rocks -> Stream.of(1, 3).map(p -> Arguments.of(type, rocks, p))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void mapsAndCallbacksSurviveTwoTaskRestores(String type, boolean rocks, int parallelism) {
    var dtype = FlinkMapScalarUdfSqlHarnessTest.rowType(type);
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                FlinkMapScalarUdfSqlHarnessTest.rows(type),
                ExternalTypeInfo.<Row>of(dtype),
                Schema.newBuilder().fromRowDataType(dtype).build(),
                false,
                12,
                32)
            .withRescaleDeployment(parallelism, null, null)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> setup =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "5");
            return FlinkMapScalarUdfSqlHarnessTest.register(table, type);
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> setup.apply(recovery.uninterrupted()),
              () -> setup.apply(recovery.get()),
              "SELECT id,map_udf(p),map_udf(p) FROM recovery_input");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(48, runs.host().rows().size());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
    }
  }
}
