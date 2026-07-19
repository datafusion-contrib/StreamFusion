package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaSinkSqlPlanTest {

  @Test
  void plansNativeSerializationWithStockExactlyOnceKafka() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING, ts TIMESTAMP(3)) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, name STRING, ts TIMESTAMP(3)) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json', "
            + "'sink.delivery-guarantee' = 'exactly-once', "
            + "'sink.transactional-id-prefix' = 'streamfusion-test')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
    assertTrue(plan.contains("native-kafka-exactly-once-sink"), plan);
  }

  @Test
  void plansTheVerifiedScalarJsonFamily() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (amount DECIMAL(10, 2), payload BYTES, event_day DATE, tod TIME(3), "
            + "instant TIMESTAMP_LTZ(3)) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (amount DECIMAL(10, 2), payload BYTES, event_day DATE, tod TIME(3), "
            + "instant TIMESTAMP_LTZ(3)) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    // Without exactly-once, the sink keeps the encode-only shape feeding Flink's own KafkaSink.
    assertFalse(plan.contains("native-kafka-exactly-once-sink"), plan);
  }

  @Test
  void recordsWhyAnUnverifiedJsonTypeFallsBack() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, items ARRAY<INT>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, items ARRAY<INT>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    table.explainSql("INSERT INTO output SELECT * FROM src");

    assertEquals(0, scan.substitutions());
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("ARRAY")),
        scan::explainSummary);
  }

  @Test
  void plansUpdatingResultsThroughNativeUpsertSerialization() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'upsert-kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'key.format' = 'json', "
            + "'value.format' = 'json', "
            + "'sink.delivery-guarantee' = 'exactly-once', "
            + "'sink.transactional-id-prefix' = 'streamfusion-upsert-test')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
    assertTrue(plan.contains("native-kafka-exactly-once-sink"), plan);
  }

  private static StreamTableEnvironment environment() {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    return StreamTableEnvironment.create(environment);
  }
}
