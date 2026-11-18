package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Routing boundary for native decoding downstream of Flink's KafkaSource. */
@Tag("streamfusion-kafka")
class KafkaWatermarkRoutingTest {

  @Test
  void supportedWatermarkedTableUsesNativeDecode() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable("json"));
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String plan = tEnv.explainSql("SELECT id, price FROM events");
    assertEquals(0, scan.fallbackReasons().size(), scan.explainSummary());
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @ParameterizedTest
  @MethodSource("calendarWatermarks")
  void calendarDelaysUseNativeDecode(String interval, boolean epochMillis) {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable(interval, epochMillis));
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String plan = tEnv.explainSql("SELECT id, price FROM events");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
    assertTrue(plan.contains("watermarkExpression=["), plan);
    assertEquals(0, scan.fallbackReasons().size(), scan.explainSummary());
  }

  @ParameterizedTest
  @MethodSource("dayTimeWatermarks")
  void dayTimeDelaysUseNativeDecode(String interval, boolean epochMillis) {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable(interval, epochMillis));
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String plan = tEnv.explainSql("SELECT id, price FROM events");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
    assertEquals(0, scan.fallbackReasons().size(), scan.explainSummary());
  }

  private static Stream<Arguments> calendarWatermarks() {
    return watermarkCases(
        "INTERVAL '1' MONTH", "INTERVAL '1' YEAR", "INTERVAL '1-1' YEAR TO MONTH");
  }

  private static Stream<Arguments> dayTimeWatermarks() {
    return watermarkCases(
        "INTERVAL '31' DAY",
        "INTERVAL '1' HOUR",
        "INTERVAL '0' SECOND",
        "INTERVAL '1 02:03:04.005' DAY TO SECOND");
  }

  private static Stream<Arguments> watermarkCases(String... intervals) {
    return Stream.of(intervals)
        .flatMap(
            interval -> Stream.of(Arguments.of(interval, false), Arguments.of(interval, true)));
  }

  private static String watermarkedTable(String interval, boolean epochMillis) {
    String ddl = watermarkedTable("json").replace("INTERVAL '4' SECOND", interval);
    return epochMillis
        ? ddl.replace("ts TIMESTAMP_LTZ(3)", "epoch BIGINT, ts AS TO_TIMESTAMP_LTZ(epoch, 3)")
        : ddl;
  }

  @Test
  void watermarkedCdcTableStaysOnFlink() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(watermarkedTable("debezium-json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, price FROM events");
    assertFalse(plan.contains("NativeKafkaDecode"), plan);
    assertTrue(plan.contains("not supported on the CDC changelog path"), plan);
  }

  @Test
  void unsupportedWatermarkEmissionPolicyStaysOnFlink() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        watermarkedTable("json")
            .replace(
                "'format' = 'json'",
                "'format' = 'json', 'scan.watermark.emit.strategy' = 'on-event'"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, price FROM events");
    assertFalse(plan.contains("NativeKafkaDecode"), plan);
    assertTrue(plan.contains("outside the native bounded-out-of-orderness contract"), plan);
  }

  @Test
  void unwatermarkedTableUsesFlinkSourceAndNativeDecode() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        "CREATE TABLE plain (id BIGINT, price BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String plan = tEnv.explainSql("SELECT id, price FROM plain WHERE price > 5");
    assertEquals(
        0, scan.fallbackReasons().size(), "no fallback expected: " + scan.fallbackReasons());
    assertTrue(scan.substitutions() >= 1, "unwatermarked table should accelerate");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  private static StreamTableEnvironment env() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return StreamTableEnvironment.create(
        env, EnvironmentSettings.newInstance().inStreamingMode().build());
  }

  private static String watermarkedTable(String format) {
    return "CREATE TABLE events ("
        + " id BIGINT, price BIGINT, ts TIMESTAMP_LTZ(3),"
        + " WATERMARK FOR ts AS ts - INTERVAL '4' SECOND"
        + ") WITH ("
        + " 'connector' = 'kafka', 'topic' = 't',"
        + " 'properties.bootstrap.servers' = 'localhost:9092',"
        + " 'scan.startup.mode' = 'earliest-offset', 'format' = '"
        + format
        + "')";
  }
}
