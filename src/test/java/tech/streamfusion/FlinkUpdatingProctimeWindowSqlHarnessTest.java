package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.ZoneOffset;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkUpdatingProctimeWindowSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "TUMBLE,ONE_PHASE,false",
    "TUMBLE,TWO_PHASE,false",
    "HOP,ONE_PHASE,false",
    "HOP,TWO_PHASE,false",
    "CUMULATE,ONE_PHASE,false",
    "CUMULATE,TWO_PHASE,false",
    "TUMBLE,ONE_PHASE,true",
    "TUMBLE,TWO_PHASE,true",
    "HOP,ONE_PHASE,true",
    "HOP,TWO_PHASE,true",
    "CUMULATE,ONE_PHASE,true",
    "CUMULATE,TWO_PHASE,true"
  })
  void signedTypedAggregatesUseOneNativeProcessingTimePhase(
      String shape, String phase, boolean attached) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String window =
          shape.equals("TUMBLE")
              ? "TUMBLE(TABLE changes,DESCRIPTOR(pt),INTERVAL '1' HOUR)"
              : shape + "(TABLE changes,DESCRIPTOR(pt),INTERVAL '1' HOUR,INTERVAL '2' HOUR)";
      String sql =
          "SELECT k,window_start,window_end,COUNT(*),COUNT(v),SUM(v),AVG(v),MIN(CAST(v AS"
              + " STRING)),MAX(CAST(v AS DECIMAL(38,8))),MIN(v) FILTER(WHERE v>100"
              + (attached ? " AND window_start<TIMESTAMP '2100-01-01 00:00:00'" : "")
              + ") FROM TABLE("
              + window
              + ") GROUP BY k,window_start,window_end";
      var comparison = NativeFailureParity.run(() -> environment(metrics, phase), sql);
      if (!tech.streamfusion.compat.FlinkTestCapabilities.RETRACTING_WINDOW_TVF) {
        comparison.assertFailure(
            TableException.class,
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.UNPLANNED);
        return;
      }
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      String operator = attached ? "NativeColumnarGroupAggregate" : "NativeColumnarWindowAggregate";
      assertTrue(comparison.nativeRun().plan().contains(operator), comparison.nativeRun().plan());
      assertFalse(comparison.nativeRun().plan().contains("NativeColumnarLocalWindowAggregate"));
      assertFalse(comparison.nativeRun().plan().contains("NativeColumnarGlobalWindowAggregate"));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), operator);
      if (attached)
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeWindowTableFunction");
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics, String phase) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    var source =
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
            Row.ofKind(RowKind.INSERT, 1, 5L),
            Row.ofKind(RowKind.UPDATE_BEFORE, 1, 5L),
            Row.ofKind(RowKind.UPDATE_AFTER, 1, 9L),
            Row.ofKind(RowKind.INSERT, 2, null),
            Row.ofKind(RowKind.INSERT, 3, 1L),
            Row.ofKind(RowKind.DELETE, 3, 1L));
    table.createTemporaryView(
        "changes",
        table.fromChangelogStream(
            source,
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", DataTypes.BIGINT())
                .columnByExpression("pt", "PROCTIME()")
                .build(),
            ChangelogMode.all()));
    return table;
  }
}
