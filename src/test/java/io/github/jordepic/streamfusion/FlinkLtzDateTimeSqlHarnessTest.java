package io.github.jordepic.streamfusion;

import java.time.Instant;
import java.time.ZoneId;
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
 * {@code DATE_FORMAT}/{@code EXTRACT} over a {@code TIMESTAMP_LTZ} — Nexmark q10/q14/q15/q16/q17 read
 * this way from a Kafka source (an epoch-millis decode lifted by {@code TO_TIMESTAMP_LTZ}). Unlike a
 * plain {@code TIMESTAMP}, an LTZ value's calendar fields depend on the session time zone, so the naive
 * "format the millis as UTC" native path would be wrong. The session zone here is {@code
 * America/New_York} (a DST zone), and the two rows straddle the DST boundary (July = UTC−4, January =
 * UTC−5), so a run that ignored the zone — or applied a single fixed offset — would diverge.
 *
 * <p>The default path routes the LTZ case through Flink's own {@code DateTimeUtils} via the JVM upcall
 * (byte-identical). The opt-in {@code allowIncompatible} path runs it natively via chrono-tz; for these
 * in-range dates it matches too, so both are held to full parity here.
 */
class FlinkLtzDateTimeSqlHarnessTest {

  @Test
  void dateFormatLtzDefaultMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkLtzDateTimeSqlHarnessTest::environment,
        "SELECT id, DATE_FORMAT(rt, 'yyyy-MM-dd') AS d, DATE_FORMAT(rt, 'yyyy-MM-dd HH:mm:ss') AS dt"
            + " FROM t");
  }

  @Test
  void extractLtzDefaultMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkLtzDateTimeSqlHarnessTest::environment,
        "SELECT id, `HOUR`(rt) AS h, `YEAR`(rt) AS y, EXTRACT(DAY FROM rt) AS dy FROM t");
  }

  @Test
  void dateFormatLtzNativeMatchesHost() throws Exception {
    withAllowIncompatible(
        () ->
            NativeParity.assertParity(
                FlinkLtzDateTimeSqlHarnessTest::environment,
                "SELECT id, DATE_FORMAT(rt, 'yyyy-MM-dd') AS d,"
                    + " DATE_FORMAT(rt, 'yyyy-MM-dd HH:mm:ss') AS dt FROM t"));
  }

  @Test
  void extractLtzNativeMatchesHost() throws Exception {
    withAllowIncompatible(
        () ->
            NativeParity.assertParity(
                FlinkLtzDateTimeSqlHarnessTest::environment,
                "SELECT id, `HOUR`(rt) AS h, `YEAR`(rt) AS y, EXTRACT(DAY FROM rt) AS dy FROM t"));
  }

  @FunctionalInterface
  private interface Body {
    void run() throws Exception;
  }

  private static void withAllowIncompatible(Body body) throws Exception {
    String key = "streamfusion.expression.allowIncompatible";
    String previous = System.getProperty(key);
    System.setProperty(key, "true");
    try {
      body.run();
    } finally {
      if (previous == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, previous);
      }
    }
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("America/New_York"));
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"id", "rt"}, Types.LONG, Types.INSTANT),
            Row.of(1L, Instant.parse("2024-07-01T12:00:00Z")), // DST: NY = 08:00, UTC−4
            Row.of(2L, Instant.parse("2024-01-01T12:00:00Z")), // standard: NY = 07:00, UTC−5
            Row.of(3L, Instant.parse("2024-07-01T02:30:00Z")), // NY = prior day 22:30 — date shifts back
            Row.of(4L, (Instant) null));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .build());
    return tEnv;
  }
}
