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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IntervalJoinAdmissionTest {
  @ParameterizedTest
  @ValueSource(strings = {"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"})
  void positiveMinimumCleanupIntervalKeepsTheEntireIslandOnFlink(String join) {
    StreamTableEnvironment table = environment(Duration.ofSeconds(1));
    PhysicalPlanScan scan = NativePlanner.install(table);

    String plan = table.explainSql(query(join));

    assertFalse(plan.contains("NativeIntervalJoin"), plan);
    assertFalse(plan.contains("RowDataToArrow"), plan);
    assertEquals(0, scan.substitutions(), plan);
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("table.exec.interval-join.min-cleanup-interval")),
        scan.explainSummary());
  }

  @ParameterizedTest
  @ValueSource(strings = {"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"})
  void defaultCleanupIntervalKeepsAllJoinKindsNative(String join) {
    StreamTableEnvironment table = environment(Duration.ZERO);
    PhysicalPlanScan scan = NativePlanner.install(table);

    String plan = table.explainSql(query(join));

    assertTrue(plan.contains("NativeIntervalJoin"), plan);
    assertTrue(scan.substitutions() > 0, plan);
  }

  private static String query(String join) {
    return "SELECT a.k, a.rt, b.rt FROM a "
        + join
        + " b ON a.k = b.k AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND"
        + " AND b.rt + INTERVAL '1' SECOND";
  }

  private static StreamTableEnvironment environment(Duration minimumCleanupInterval) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig()
        .set(
            ExecutionConfigOptions.TABLE_EXEC_INTERVAL_JOIN_MIN_CLEAN_UP_INTERVAL,
            minimumCleanupInterval);
    for (String name : new String[] {"a", "b"}) {
      table.createTemporaryView(
          name,
          fromData(
              env,
              Types.ROW_NAMED(new String[] {"k", "rt"}, Types.LONG, Types.LOCAL_DATE_TIME),
              Row.of(1L, LocalDateTime.of(2024, 1, 1, 0, 0))),
          Schema.newBuilder()
              .column("k", DataTypes.BIGINT())
              .column("rt", DataTypes.TIMESTAMP(3))
              .watermark("rt", "rt - INTERVAL '1' SECOND")
              .build());
    }
    return table;
  }
}
