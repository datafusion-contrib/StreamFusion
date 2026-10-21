package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkUpdatingAttachedWindowRecoveryTest {
  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,hashmap,STRING", "TWO_PHASE,hashmap,STRING", "ONE_PHASE,hashmap,'DECIMAL(38,8)'",
        "TWO_PHASE,hashmap,'DECIMAL(38,8)'", "ONE_PHASE,hashmap,TIMESTAMP(9)",
        "TWO_PHASE,hashmap,TIMESTAMP(9)",
    "ONE_PHASE,tech.streamfusion.state.RocksDBNativeStateBackendFactory,STRING",
        "TWO_PHASE,tech.streamfusion.state.RocksDBNativeStateBackendFactory,STRING",
        "ONE_PHASE,tech.streamfusion.state.RocksDBNativeStateBackendFactory,'DECIMAL(38,8)'",
        "TWO_PHASE,tech.streamfusion.state.RocksDBNativeStateBackendFactory,'DECIMAL(38,8)'",
        "ONE_PHASE,tech.streamfusion.state.RocksDBNativeStateBackendFactory,TIMESTAMP(9)",
        "TWO_PHASE,tech.streamfusion.state.RocksDBNativeStateBackendFactory,TIMESTAMP(9)"
  })
  void signedViewsAndCachedExtremesSurviveTwoRestores(String phase, String backend, String type)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.RETRACTING_WINDOW_TVF);
    var rows = new ArrayList<Row>();
    for (int index = 0; index < 96; index++) {
      int original = index >= 64 ? index - 64 : index;
      rows.add(
          Row.ofKind(
              index >= 64 ? RowKind.DELETE : RowKind.INSERT,
              original % 3,
              original % 11 == 0 ? null : (long) (original % 17),
              LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
              LocalDateTime.ofEpochSecond(1, original % 13, ZoneOffset.UTC)));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "v", "rt", "tv"},
                    Types.INT,
                    Types.LONG,
                    Types.LOCAL_DATE_TIME,
                    Types.LOCAL_DATE_TIME),
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("v", DataTypes.BIGINT())
                    .column("rt", DataTypes.TIMESTAMP(3))
                    .column("tv", DataTypes.TIMESTAMP(9))
                    .watermark("rt", "SOURCE_WATERMARK()")
                    .build(),
                true,
                24,
                72)
            .withWatermarks(Map.of(96, Long.MAX_VALUE))
            .withExecutionMetrics()) {
      String value = type.equals("TIMESTAMP(9)") ? "tv" : "CAST(v AS " + type + ")";
      String sql =
          "SELECT k,window_end,MIN("
              + value
              + "),MAX("
              + value
              + "),COUNT(*) FILTER(WHERE v>100 AND window_start<TIMESTAMP '1970-01-01"
              + " 00:00:05'),MIN("
              + value
              + ") FILTER(WHERE v<0) FROM TABLE(HOP(TABLE recovery_input,DESCRIPTOR(rt),INTERVAL"
              + " '5' SECOND,INTERVAL '10' SECOND)) GROUP BY k,window_start,window_end";
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), phase),
              () -> configure(recovery.get(), phase),
              sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(),
          phase.equals("TWO_PHASE")
              ? "NativeColumnarGlobalWindowAggregate"
              : "NativeColumnarWindowAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeWindowTableFunction");
      SharedFlinkCluster.assertNativeMemoryReleased("updating-attached-window-recovery");
    }
  }

  private static TableEnvironment configure(TableEnvironment table, String phase) {
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    return table;
  }
}
