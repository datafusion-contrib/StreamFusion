package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Plan-time routing for watermarked Kafka tables (no broker needed — the source is built at execution,
 * not planning). Flink pushes a table's {@code WATERMARK} clause into the Kafka scan (the connector
 * supports watermark push-down), so no assigner node survives in the plan: whatever replaces the scan
 * must regenerate the watermarks. The native source decodes inside the poll, so it holds rowtimes and
 * regenerates the supported shapes per split; everything else — an unreproducible shape, the CDC
 * formats, or the decode-operator path (downstream of Flink's source) — must stay on Flink rather than
 * silently losing its watermarks, a bug bounded runs mask because the final MAX_WATERMARK closes all
 * windows regardless.
 */
@Tag("streamfusion-kafka")
class KafkaWatermarkRoutingTest {

  private static final String QUERY =
      "SELECT window_start, SUM(price) FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts),"
          + " INTERVAL '1' MINUTE)) GROUP BY window_start";

  @AfterEach
  void clearFlags() {
    System.clearProperty("streamfusion.operator.kafkaSource.enabled");
  }

  @Test
  void watermarkedTableRoutesToTheNativeSource() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable(""));
    String plan = NativePlanner.explain(tEnv, QUERY);
    assertTrue(
        plan.contains("NativeKafkaSource(topic="),
        "watermarked JSON table should take the native source:\n" + plan);
    assertTrue(
        plan.contains("watermark=[ts - 4000ms]"),
        "the source node should carry the pushed watermark it regenerates:\n" + plan);
  }

  @Test
  void watermarkedTableFallsBackWithReasonWhenNativeSourceIsOff() {
    // With the source gate off (or an opt-out native build), the decode path must NOT take the
    // table (it regenerates no watermarks — an unbounded event-time query would never emit), and
    // the fallback reason should say exactly why.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable(""));
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "false");
    String plan = NativePlanner.explain(tEnv, QUERY);
    assertTrue(
        !plan.contains("NativeKafkaDecode") && !plan.contains("NativeKafkaSource"),
        "a watermarked table must not route without watermark regeneration:\n" + plan);
    assertTrue(
        plan.contains("only the native source regenerates the pushed WATERMARK"),
        "expected the precise watermark fallback reason:\n" + plan);
  }

  @Test
  void computedEpochMillisRowtimeRoutesToTheNativeSource() {
    // The common Kafka-table idiom (and the Nexmark harness's): a physical BIGINT of epoch millis
    // with `rowtime AS TO_TIMESTAMP_LTZ(dateTime, 3)`. The push-down leaves the scan emitting only
    // physical columns; the watermark reads the bigint verbatim (it already is the rowtime millis).
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        "CREATE TABLE events (id BIGINT, price BIGINT, `dateTime` BIGINT,"
            + " rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3),"
            + " WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND"
            + ") WITH ('connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')");
    String plan =
        NativePlanner.explain(
            tEnv,
            "SELECT window_start, SUM(price) FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(rowtime),"
                + " INTERVAL '1' MINUTE)) GROUP BY window_start");
    assertTrue(
        plan.contains("NativeKafkaSource(topic="),
        "computed-rowtime table should take the native source:\n" + plan);
    assertTrue(
        plan.contains("watermark=[dateTime - 4000ms]"),
        "the source node should read the epoch-millis column as the rowtime:\n" + plan);
  }

  @Test
  void onEventEmitStrategyIsDeclinedEvenWithNativeSourceOn() {
    // On-event emit would need a watermark between rows of one Arrow batch — outside the
    // reproducible shapes, so the whole scan stays on Flink.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable(", 'scan.watermark.emit.strategy' = 'on-event'"));
    String plan = NativePlanner.explain(tEnv, QUERY);
    assertTrue(
        !plan.contains("NativeKafkaDecode") && !plan.contains("NativeKafkaSource"),
        "an on-event watermark must not route:\n" + plan);
    assertTrue(
        plan.contains("isn't a shape the native source reproduces"),
        "expected the unsupported-shape fallback reason:\n" + plan);
  }

  @Test
  void watermarkedCdcTableFallsBack() {
    // CDC formats decode in an operator downstream of Flink's source, which cannot regenerate the
    // pushed watermark — a watermarked CDC table stays on Flink entirely.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable("").replace("'format' = 'json'", "'format' = 'debezium-json'"));
    String plan = NativePlanner.explain(tEnv, QUERY);
    assertTrue(
        !plan.contains("NativeKafkaDecode") && !plan.contains("NativeKafkaSource"),
        "a watermarked CDC table must not route:\n" + plan);
    assertTrue(
        plan.contains("the CDC decode runs downstream of the source"),
        "expected the CDC watermark fallback reason:\n" + plan);
  }

  @Test
  void unwatermarkedTableStillTakesTheDecodePath() {
    // Pin the source gate off: this guards the decode-path routing, which serves the formats and
    // builds the native source doesn't.
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "false");
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        "CREATE TABLE plain (id BIGINT, price BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.explainSql("SELECT id, price FROM plain WHERE price > 5");
    assertEquals(0, scan.fallbackReasons().size(), "no fallback expected: " + scan.fallbackReasons());
    assertTrue(scan.substitutions() >= 1, "unwatermarked table should still accelerate");
  }

  private static StreamTableEnvironment env() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return StreamTableEnvironment.create(
        env, EnvironmentSettings.newInstance().inStreamingMode().build());
  }

  private static String watermarkedTable(String extraOptions) {
    return "CREATE TABLE events ("
        + " id BIGINT, price BIGINT, ts TIMESTAMP_LTZ(3),"
        + " WATERMARK FOR ts AS ts - INTERVAL '4' SECOND"
        + ") WITH ("
        + " 'connector' = 'kafka', 'topic' = 't',"
        + " 'properties.bootstrap.servers' = 'localhost:9092',"
        + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'json'"
        + extraOptions
        + ")";
  }
}
