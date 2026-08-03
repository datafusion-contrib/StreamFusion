package tech.streamfusion;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
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
  void perOperatorFlagKeepsLegacySessionOnHost() throws Exception {
    // The legacy session group-window shares the windowAggregate kill switch with the TVF session it
    // routes to. This is the shape Nexmark q11 is written in, so the switch is the only way to take
    // it off the native path — worth pinning, since it is the one legacy group-window we accelerate.
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

  private static TableEnvironment environment() {
    return build("ONE_PHASE");
  }

  private static TableEnvironment twoPhaseEnvironment() {
    return build("TWO_PHASE");
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
                    .withTimestampAssigner(
                        (row, ts) ->
                            ((LocalDateTime) row.getField(2)).toInstant(ZoneOffset.UTC).toEpochMilli()));
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
}
