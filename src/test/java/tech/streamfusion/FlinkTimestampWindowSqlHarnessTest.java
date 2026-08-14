package tech.streamfusion;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * Window aggregate over a plain {@code TIMESTAMP(3)} event-time attribute (the Nexmark rowtime shape —
 * a watermarked TIMESTAMP column, not a local-time-zone one). The window bounds are the raw wall-clock,
 * rendered in UTC rather than shifted through the session zone; value-compared to the host so the
 * window_start/window_end and aggregates must match exactly regardless of the JVM's default zone.
 */
class FlinkTimestampWindowSqlHarnessTest {

  private static final String TUMBLE =
      "SELECT window_start, window_end, COUNT(*) AS c, SUM(`value`) AS s, MAX(`value`) AS mx "
          + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
          + "GROUP BY window_start, window_end";

  @Test
  void tumbleOverPlainTimestampMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkTimestampWindowSqlHarnessTest::environment, TUMBLE);
  }

  @Test
  void keyedTumbleOverPlainTimestampMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, window_start, COUNT(*) AS c FROM "
            + "TABLE(TUMBLE(TABLE src, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void zeroAggregateWindowedDistinctMatchesHost() throws Exception {
    // GROUP BY key + window with NO aggregate function — a windowed distinct, one row per (k, window).
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::twoPhaseEnvironment,
        "SELECT k, window_start FROM "
            + "TABLE(TUMBLE(TABLE src, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void windowJoinOfWindowedDistinctsMatchesHost() throws Exception {
    // Nexmark q8 shape: a window join of two zero-aggregate windowed distincts on key + window, over
    // a plain TIMESTAMP rowtime.
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::twoPhaseEnvironment,
        "SELECT a.k, a.window_start FROM "
            + "(SELECT k, window_start, window_end FROM "
            + "  TABLE(TUMBLE(TABLE src, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
            + "  GROUP BY k, window_start, window_end) a "
            + "JOIN (SELECT k, window_start, window_end FROM "
            + "  TABLE(TUMBLE(TABLE src, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
            + "  GROUP BY k, window_start, window_end) b "
            + "ON a.k = b.k AND a.window_start = b.window_start AND a.window_end = b.window_end");
  }

  @Test
  void twoPhaseTumbleOverPlainTimestampMatchesHost() throws Exception {
    // Two-phase (local pre-aggregate + global merge): the global renders the window bounds, so its
    // UTC render for a plain TIMESTAMP must match the host too.
    NativeParity.assertParity(FlinkTimestampWindowSqlHarnessTest::twoPhaseEnvironment, TUMBLE);
  }

  @Test
  void legacySessionGroupWindowMatchesHost() throws Exception {
    // The legacy GROUP BY k, SESSION(...) syntax (StreamPhysicalGroupWindowAggregate) routed to the
    // native session operator. Output [k, count, session_start, session_end]; the rowtime/proctime
    // window properties Flink also appends are emitted and projected away.
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, COUNT(*) AS c, "
            + "SESSION_START(ts, INTERVAL '10' SECOND) AS starttime, "
            + "SESSION_END(ts, INTERVAL '10' SECOND) AS endtime "
            + "FROM src GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)");
  }

  @Test
  void legacyTumbleGroupWindowMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, COUNT(*) AS c, "
            + "TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime, "
            + "TUMBLE_END(ts, INTERVAL '10' SECOND) AS endtime "
            + "FROM src GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
  }

  @Test
  void legacyGlobalTumbleGroupWindowMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT COUNT(*) AS c, TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime "
            + "FROM src GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)");
  }

  @Test
  void legacyZeroAggregateWindowedDistinctMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime "
            + "FROM src GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
  }

  @Test
  void legacyHopGroupWindowMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, SUM(`value`) AS s, "
            + "HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND) AS starttime, "
            + "HOP_END(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND) AS endtime "
            + "FROM src GROUP BY k, HOP(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND)");
  }

  @Test
  void legacyGappedHopGroupWindowMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, SUM(`value`) AS s, "
            + "HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '2' SECOND) AS starttime, "
            + "HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '2' SECOND) AS endtime "
            + "FROM src GROUP BY k, HOP(ts, INTERVAL '5' SECOND, INTERVAL '2' SECOND)");
  }

  @Test
  void legacyTumbleWithoutWindowPropertiesMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, COUNT(*) AS c FROM src GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
  }

  @Test
  void legacyTumbleTimePropertiesMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkTimestampWindowSqlHarnessTest::environment,
        "SELECT k, COUNT(*) AS c, "
            + "TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime, "
            + "TUMBLE_END(ts, INTERVAL '10' SECOND) AS endtime, "
            + "TUMBLE_ROWTIME(ts, INTERVAL '10' SECOND) AS rowtime "
            + "FROM src GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
  }

  @Test
  void perOperatorFlagKeepsLegacyGroupWindowOnHost() throws Exception {
    // Legacy fixed and session group windows share the windowAggregate kill switch with their TVF
    // equivalents. Pin one legacy shape so registry changes cannot accidentally bypass the switch.
    System.setProperty("streamfusion.operator.windowAggregate.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkTimestampWindowSqlHarnessTest::environment,
          "SELECT k, COUNT(*) AS c, "
              + "SESSION_START(ts, INTERVAL '10' SECOND) AS starttime, "
              + "SESSION_END(ts, INTERVAL '10' SECOND) AS endtime "
              + "FROM src GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)",
          "windowAggregate: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.windowAggregate.enabled");
    }
  }

  @Test
  void legacyEarlyFireWindowFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkTimestampWindowSqlHarnessTest::earlyFireEnvironment,
        "SELECT k, COUNT(*) AS c, TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime "
            + "FROM src GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)",
        "early/late firing");
  }

  @Test
  void legacyLateFireWindowFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkTimestampWindowSqlHarnessTest::lateFireEnvironment,
        "SELECT k, COUNT(*) AS c, TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime "
            + "FROM src GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)",
        "early/late firing");
  }

  @Test
  void legacyWindowOverRetractingInputFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkTimestampWindowSqlHarnessTest::retractingEnvironment,
        "SELECT k, SUM(`value`) AS s, TUMBLE_START(ts, INTERVAL '10' SECOND) AS starttime "
            + "FROM changes GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)",
        "retracting or updating input");
  }

  private static TableEnvironment environment() {
    return build("ONE_PHASE");
  }

  private static TableEnvironment twoPhaseEnvironment() {
    return build("TWO_PHASE");
  }

  private static TableEnvironment earlyFireEnvironment() {
    TableEnvironment tEnv = environment();
    tEnv.getConfig().set("table.exec.emit.early-fire.enabled", "true");
    tEnv.getConfig().set("table.exec.emit.early-fire.delay", "0 ms");
    return tEnv;
  }

  private static TableEnvironment lateFireEnvironment() {
    TableEnvironment tEnv = environment();
    tEnv.getConfig().set("table.exec.emit.late-fire.enabled", "true");
    tEnv.getConfig().set("table.exec.emit.late-fire.delay", "0 ms");
    tEnv.getConfig().set("table.exec.emit.allow-lateness", "1 s");
    return tEnv;
  }

  private static TableEnvironment retractingEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "value", "ts"},
                    Types.LONG,
                    Types.LONG,
                    Types.LOCAL_DATE_TIME),
                Row.ofKind(
                    RowKind.INSERT, 1L, 5L, LocalDateTime.of(2024, 6, 1, 12, 0, 1)),
                Row.ofKind(
                    RowKind.UPDATE_BEFORE, 1L, 5L, LocalDateTime.of(2024, 6, 1, 12, 0, 1)),
                Row.ofKind(
                    RowKind.UPDATE_AFTER, 1L, 7L, LocalDateTime.of(2024, 6, 1, 12, 0, 1)))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((row, timestamp) -> timestampMillis(row)));
    Table table =
        tEnv.fromChangelogStream(
            source,
            Schema.newBuilder()
                .column("k", DataTypes.BIGINT())
                .column("value", DataTypes.BIGINT())
                .column("ts", DataTypes.TIMESTAMP(3))
                .watermark("ts", "SOURCE_WATERMARK()")
                .build(),
            ChangelogMode.all());
    tEnv.createTemporaryView("changes", table);
    return tEnv;
  }

  private static TableEnvironment build(String phaseStrategy) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", phaseStrategy);
    // ts is a plain TIMESTAMP(3) rowtime attribute (not local-time-zone); the source carries the
    // watermarks (SOURCE_WATERMARK), so no interior watermark-assigner breaks the columnar island.
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "value", "ts"},
                    Types.LONG,
                    Types.LONG,
                    Types.LOCAL_DATE_TIME),
                Row.of(1L, 5L, LocalDateTime.of(2024, 6, 1, 12, 0, 1)),
                Row.of(1L, 7L, LocalDateTime.of(2024, 6, 1, 12, 0, 3)),
                Row.of(2L, 9L, LocalDateTime.of(2024, 6, 1, 12, 0, 4)),
                Row.of(1L, 2L, LocalDateTime.of(2024, 6, 1, 12, 0, 13)),
                Row.of(2L, 8L, LocalDateTime.of(2024, 6, 1, 12, 0, 25)))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, timestamp) -> timestampMillis(row)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT())
            .column("ts", DataTypes.TIMESTAMP(3))
            .watermark("ts", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }

  private static long timestampMillis(Row row) {
    return ((LocalDateTime) Objects.requireNonNull(row.getField(2)))
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli();
  }
}
