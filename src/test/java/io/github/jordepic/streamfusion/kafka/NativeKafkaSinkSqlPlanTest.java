package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
    table.explainSql("INSERT INTO output SELECT * FROM src");

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  @Test
  void recordsWhyAnUnverifiedJsonTypeFallsBack() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, amount DECIMAL(10, 2)) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, amount DECIMAL(10, 2)) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    table.explainSql("INSERT INTO output SELECT * FROM src");

    assertEquals(0, scan.substitutions());
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("DECIMAL")),
        scan::explainSummary);
  }

  private static StreamTableEnvironment environment() {
    StreamExecutionEnvironment environment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    return StreamTableEnvironment.create(environment);
  }
}
