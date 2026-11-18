package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SingletonGlobalWindowRecoveryTest {
  @ParameterizedTest(name = "singleton recovery: rocks={0}, unaligned={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void cumulativeGlobalRestoresSingleKeyGroupTwice(boolean rocks, boolean unaligned) {
    var input = new ArrayList<Row>();
    var start = LocalDateTime.of(1970, 1, 1, 0, 0);
    for (int i = 0; i < 12; i++) input.add(Row.of(start.plusSeconds(i), (long) i + 1));
    var type = Types.ROW_NAMED(new String[] {"t", "v"}, Types.LOCAL_DATE_TIME, Types.LONG);
    var schema = Schema.newBuilder()
        .column("t", DataTypes.TIMESTAMP(3))
        .column("v", DataTypes.BIGINT())
        .watermark("t", "SOURCE_WATERMARK()")
        .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery = new PortableSqlRecovery(backend, input, type, schema, false, 4, 8)
        .withExecutionMetrics()
        .withWatermarks(Map.of(4, 2999L, 8, 6999L, 12, 59999L))) {
      var runs = NativeFailureParity.runRecovery(
          () -> configure(recovery, false, unaligned),
          () -> configure(recovery, true, unaligned),
          "SELECT window_start, window_end, SUM(v) FROM TABLE(CUMULATE(TABLE recovery_input, "
              + "DESCRIPTOR(t), INTERVAL '10' SECOND, INTERVAL '60' SECOND)) "
              + "GROUP BY window_start, window_end");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(runs.nativeRun().plan().contains("NativeColumnarGlobalWindowAggregate"));
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalWindowAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGlobalWindowAggregate");
      recovery.verifyRepeatedRecovery();
    }
  }

  private static TableEnvironment configure(
      PortableSqlRecovery recovery, boolean recover, boolean unaligned) {
    var table = recover ? recovery.get() : recovery.uninterrupted();
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    recovery.executionEnvironment().getCheckpointConfig().enableUnalignedCheckpoints(unaligned);
    return table;
  }
}
