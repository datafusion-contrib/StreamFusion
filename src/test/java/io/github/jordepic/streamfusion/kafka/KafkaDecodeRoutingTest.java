package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Plan-time gating for the native Kafka decode paths (no broker needed): a table whose schema or
 * format options the native decoders cannot reproduce with Flink's exact semantics must keep its
 * scan on Flink, rather than reach a decode that would produce wrong values (a metadata column
 * silently NULL) or fail at runtime (a column type the appender dispatch panics on). The admitted
 * counterparts pin the gates to their exact boundary — each fallback test has a twin proving the
 * same table routes once the offending piece is removed.
 */
@Tag("streamfusion-kafka")
class KafkaDecodeRoutingTest {

  @Test
  void metadataColumnKeepsTheScanOnFlink() {
    // `ts` is filled by the connector from the record's Kafka timestamp; a native value decode
    // would look for a "ts" key in the message body and emit NULL.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'", "json"));
    assertStaysOnFlink(tEnv, "SELECT id, name, ts FROM t");
  }

  @Test
  void unreferencedMetadataColumnAlsoKeepsTheScanOnFlink() {
    // Flink does not prune a declared metadata column from the Kafka scan even when the query never
    // reads it — the scan still produces it, so the gate (which keys on what the scan produces)
    // declines regardless of the projection.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'", "json"));
    assertStaysOnFlink(tEnv, "SELECT id, name FROM t");
  }

  @Test
  void metadataColumnGateAppliesToEveryInsertOnlyFormat() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'", "csv"));
    assertStaysOnFlink(tEnv, "SELECT id, name, ts FROM t");
  }

  @Test
  void timeAndVarbinaryColumnsRoute() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, t TIME(0), t3 TIME(3), b VARBINARY(1024)", "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, t, t3, b FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode") || plan.contains("NativeKafkaSource"),
        "TIME and VARBINARY columns should decode natively:\n" + plan);
  }

  @Test
  void binaryColumnKeepsTheScanOnFlink() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, b BINARY(4)", "json"));
    assertStaysOnFlink(tEnv, "SELECT id, b FROM t");
  }

  @Test
  void nestedUnsupportedLeafKeepsTheScanOnFlink() {
    // The type gate must recurse: the unsupported leaf hides inside a ROW column.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, nested ROW<a BIGINT, b BINARY(4)>", "json"));
    assertStaysOnFlink(tEnv, "SELECT id, nested FROM t");
  }

  @Test
  void nonStringMapKeyFailsPlanningOnFlinkItself() {
    // Flink's JSON format factory rejects a non-string map key while the scan is being planned, so
    // the query never reaches substitution — the native gate for this shape is purely defensive
    // (it protects any non-SQL entry to the provider). Pin Flink's behavior so a change would
    // surface here.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, m MAP<INT, STRING>", "json"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> NativePlanner.explain(tEnv, "SELECT id, m FROM t"));
  }

  @Test
  void treeDeserializerOptionKeepsTheScanOnFlink() {
    // decode.json-parser.enabled = false switches Flink to its tree deserializer, whose coercions
    // differ from the parser path the native decode mirrors.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "json")
            .replace("'json')", "'json', 'json.decode.json-parser.enabled' = 'false')"));
    assertStaysOnFlink(tEnv, "SELECT id, name FROM t");
  }

  @Test
  void cdcColumnTypeGateAppliesToTheDialects() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, b BINARY(4)", "debezium-json"));
    assertStaysOnFlink(tEnv, "SELECT id, b FROM t");
  }

  @Test
  void debeziumAvroConfluentRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "debezium-avro-confluent")
            .replace(
                "'debezium-avro-confluent')",
                "'debezium-avro-confluent', 'debezium-avro-confluent.url' = 'http://localhost:8081')"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a plain-URL debezium-avro-confluent table should decode natively:\n" + plan);
  }

  @Test
  void debeziumAvroConfluentRegistryAuthKeepsTheScanOnFlink() {
    // Registry auth/SSL/client-properties options aren't translated to the plain-HTTP native
    // fetch — the same fallback set as avro-confluent.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "debezium-avro-confluent")
            .replace(
                "'debezium-avro-confluent')",
                "'debezium-avro-confluent', 'debezium-avro-confluent.url' = 'http://localhost:8081',"
                    + " 'debezium-avro-confluent.basic-auth.user-info' = 'user:pw')"));
    assertStaysOnFlink(tEnv, "SELECT id, name FROM t");
  }


  @Test
  void csvComplexColumnKeepsTheScanOnFlink() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, nums ARRAY<BIGINT>", "csv"));
    assertStaysOnFlink(tEnv, "SELECT id, nums FROM t");
  }

  @Test
  void supportedSchemaStillRoutes() {
    // The twin of the gates above: the same table shapes minus the offending piece must route.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, nested ROW<a BIGINT>, m MAP<STRING, INT>", "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name, nested, m FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode") || plan.contains("NativeKafkaSource"),
        "a supported schema should route natively:\n" + plan);
  }

  private static void assertStaysOnFlink(StreamTableEnvironment tEnv, String sql) {
    String plan = NativePlanner.explain(tEnv, sql);
    assertTrue(
        !plan.contains("NativeKafkaDecode") && !plan.contains("NativeKafkaSource"),
        "the scan must stay on Flink:\n" + plan);
  }

  private static StreamTableEnvironment env() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return StreamTableEnvironment.create(
        env, EnvironmentSettings.newInstance().inStreamingMode().build());
  }

  private static String table(String columns, String format) {
    return "CREATE TABLE t ("
        + columns
        + ") WITH ("
        + " 'connector' = 'kafka', 'topic' = 't',"
        + " 'properties.bootstrap.servers' = 'localhost:9092',"
        + " 'scan.startup.mode' = 'earliest-offset', 'format' = '"
        + format
        + "')";
  }

  @Test
  void rawKeyedTableRoutesToTheDecodePath() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        keyedTable(
            "k BIGINT, id BIGINT, name STRING",
            "'key.format' = 'raw', 'key.fields' = 'k', 'value.fields-include' = 'EXCEPT_KEY'"));
    String plan = NativePlanner.explain(tEnv, "SELECT k, id, name FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a raw-keyed table should route to the decode path:\n" + plan);
    assertTrue(
        !plan.contains("NativeKafkaSource"),
        "the fused source cannot carry keys yet:\n" + plan);
  }

  @Test
  void rawKeyedTableWithDefaultAllProjectionRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        keyedTable("k BIGINT, id BIGINT", "'key.format' = 'raw', 'key.fields' = 'k'"));
    String plan = NativePlanner.explain(tEnv, "SELECT k, id FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void keyedShapesOutsideTheIncrementStayOnFlink() {
    // A non-raw key format needs the alignment machinery (a JSON key can drop the whole record).
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        keyedTable(
            "k BIGINT, id BIGINT",
            "'key.format' = 'json', 'key.fields' = 'k', 'value.fields-include' = 'EXCEPT_KEY'"));
    assertStaysOnFlink(tEnv, "SELECT k, id FROM t");
    // A non-UTF-8 key charset decodes through Java's charset machinery — stays on Flink, like the
    // value-side raw gate.
    StreamTableEnvironment charset = env();
    charset.executeSql(
        keyedTable(
            "k STRING, id BIGINT",
            "'key.format' = 'raw', 'key.fields' = 'k', 'key.raw.charset' = 'UTF-16',"
                + " 'value.fields-include' = 'EXCEPT_KEY'"));
    assertStaysOnFlink(charset, "SELECT k, id FROM t");
  }

  private static String keyedTable(String columns, String keyOptions) {
    return "CREATE TABLE t ("
        + columns
        + ") WITH ("
        + " 'connector' = 'kafka', 'topic' = 't',"
        + " 'properties.bootstrap.servers' = 'localhost:9092',"
        + " 'scan.startup.mode' = 'earliest-offset', 'value.format' = 'json', "
        + keyOptions
        + ")";
  }
}
