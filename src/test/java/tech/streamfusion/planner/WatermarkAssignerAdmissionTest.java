package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Duration;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class WatermarkAssignerAdmissionTest {
  private static final String QUERY =
      "SELECT window_start, COUNT(*) FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), "
          + "INTERVAL '1' SECOND)) GROUP BY window_start, window_end";

  @Test
  void sourceIdleTimeoutKeepsTheEntireIslandOnFlink() {
    StreamTableEnvironment table = environment(Duration.ofSeconds(1));
    PhysicalPlanScan scan = NativePlanner.install(table);

    String plan = table.explainSql(QUERY);

    assertTrue(plan.contains("WatermarkAssigner"), plan);
    assertFalse(plan.contains("NativeWatermarkAssigner"), plan);
    assertFalse(plan.contains("RowDataToArrow"), plan);
    assertEquals(0, scan.substitutions(), plan);
  }

  @Test
  void disabledIdlenessRetainsNativeWatermarksAndWindows() {
    StreamTableEnvironment table = environment(Duration.ZERO);
    PhysicalPlanScan scan = NativePlanner.install(table);

    String plan = table.explainSql(QUERY);

    assertTrue(plan.contains("NativeWatermarkAssigner"), plan);
    assertTrue(scan.substitutions() > 1, plan);
  }

  private static StreamTableEnvironment environment(Duration idleTimeout) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT, idleTimeout);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.createTemporaryView(
        "t",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"rt"}, Types.LOCAL_DATE_TIME),
            Row.of(LocalDateTime.of(2024, 1, 1, 0, 0))),
        Schema.newBuilder()
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "rt - INTERVAL '1' SECOND")
            .build());
    return table;
  }
}
