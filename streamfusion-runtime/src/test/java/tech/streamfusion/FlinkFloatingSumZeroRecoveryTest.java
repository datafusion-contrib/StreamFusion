package tech.streamfusion;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkFloatingSumZeroRecoveryTest {
  @ParameterizedTest
  @CsvSource({
    "float,false,0", "float,true,0", "double,false,0", "double,true,0",
    "float,false,2", "float,true,2", "double,false,2", "double,true,2"
  })
  void restoresRoundedZeroSumThroughNullPrefixesAndRetractingBundles(
      String type, boolean rocks, int bundle) {
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", type.equals("float") ? DataTypes.FLOAT() : DataTypes.DOUBLE()));
    double high = type.equals("float") ? 16_777_216.0 : 9_007_199_254_740_992.0;
    Object one = number(type, 1.0);
    var input =
        List.of(
            Row.of(1, (Object) null),
            Row.of(2, (Object) null),
            Row.of(1, (Object) null),
            Row.of(2, (Object) null),
            Row.of(1, number(type, high)),
            Row.of(1, one),
            Row.of(1, number(type, -high)),
            Row.ofKind(RowKind.DELETE, 1, one),
            Row.of(1, one),
            Row.of(2, number(type, -0.0)));
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                ExternalTypeInfo.<Row>of(rowType),
                Schema.newBuilder().fromRowDataType(rowType).build(),
                true,
                4,
                8)
            .withExecutionMetrics()) {
      UnaryOperator<TableEnvironment> configure =
          table -> {
            table
                .getConfig()
                .set("table.optimizer.agg-phase-strategy", bundle == 0 ? "ONE_PHASE" : "TWO_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(bundle > 0));
            table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
            table
                .getConfig()
                .set("table.exec.mini-batch.size", Integer.toString(Math.max(1, bundle)));
            table.createTemporaryView(
                "sum_zero_result",
                table
                    .from("recovery_input")
                    .groupBy($("k"))
                    .select($("k"), $("v").sum0().as("z"), $("v").count().as("n")));
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT * FROM sum_zero_result");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      if (bundle > 0)
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
    }
  }

  private static Object number(String type, double value) {
    if (type.equals("float")) return Float.valueOf((float) value);
    return Double.valueOf(value);
  }
}
