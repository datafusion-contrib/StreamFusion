package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.regex.PatternSyntaxException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

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
  void everyConnectorMetadataColumnRoutesToTheNativeDecode() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table(
            "id BIGINT, name STRING, tp STRING METADATA FROM 'topic',"
                + " part INT METADATA FROM 'partition',"
                + " hdrs MAP<STRING, BYTES> METADATA FROM 'headers',"
                + " epoch INT METADATA FROM 'leader-epoch', off BIGINT METADATA FROM 'offset',"
                + " ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',"
                + " tst STRING METADATA FROM 'timestamp-type'",
            "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT * FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode")
            && plan.contains(
                "metadata=[{tp=topic, part=partition, hdrs=headers, epoch=leader-epoch,"
                    + " off=offset, ts=timestamp, tst=timestamp-type}]"),
        "every connector metadata column should decode natively:\n" + plan);
  }

  @Test
  void unreferencedMetadataColumnRoutesToo() {
    // Flink keeps a declared metadata column in the Kafka scan even when the query never reads it.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'", "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void metadataColumnsRouteForEveryInsertOnlyFormat() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, off BIGINT METADATA FROM 'offset'", "csv"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name, off FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void valueFormatMetadataKeepsTheScanOnFlink() {
    // A CDC format's own metadata comes from the message envelope, which the decode doesn't read.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, db STRING METADATA FROM 'value.database'", "canal-json"));
    assertStaysOnFlink(tEnv, "SELECT id, name, db FROM t");
  }

  @Test
  void connectorMetadataRoutesForCdcFormats() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, off BIGINT METADATA FROM 'offset'", "canal-json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name, off FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void connectorMetadataRoutesWithCanalIncludeFilters() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING, off BIGINT METADATA FROM 'offset'", "canal-json")
            .replace(
                "'format' = 'canal-json')",
                "'format' = 'canal-json', 'canal-json.table.include' = 'orders')"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name, off FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void metadataOverAFormatThatDoesNotComposeItKeepsTheScanOnFlink() {
    // Each native format declares whether its decoder composes Kafka metadata; Avro's doesn't.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, name STRING, off BIGINT METADATA FROM 'offset'", "avro"));
    assertStaysOnFlink(tEnv, "SELECT id, name, off FROM t");
  }

  @Test
  void theSameAvroTableWithoutMetadataRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, name STRING", "avro"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void aTableOfOnlyMetadataKeepsTheScanOnFlink() {
    // The value decode counts a message's rows by its columns, and this table declares none.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("off BIGINT METADATA FROM 'offset'", "json"));
    assertStaysOnFlink(tEnv, "SELECT off FROM t");
  }

  @Test
  void theSameTableWithAPhysicalColumnRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, off BIGINT METADATA FROM 'offset'", "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, off FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void headersReadByKeyRouteOnEveryLine() {
    // Flink casts the connector's NOT NULL map into the nullable column, and a key lookup lets the
    // planner drop that cast.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, hdrs MAP<STRING, BYTES> METADATA FROM 'headers'", "json"));
    tEnv.executeSql("CREATE TABLE sink (id BIGINT, trace BYTES) WITH ('connector' = 'blackhole')");
    String plan = NativePlanner.explain(tEnv, "INSERT INTO sink SELECT id, hdrs['trace'] FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void wholeHeadersRouteWhereFlinksNullabilityCastIsTheIdentity() {
    // Emitting the map keeps the cast. 2.2 runs it as the identity; 1.18 rebuilds the map into a
    // fresh HashMap, which the native Calc doesn't reproduce, so there the job stays on Flink.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, hdrs MAP<STRING, BYTES> METADATA FROM 'headers'", "json"));
    tEnv.executeSql(
        "CREATE TABLE sink (id BIGINT, hdrs MAP<STRING, BYTES>) WITH ('connector' = 'blackhole')");
    String plan = NativePlanner.explain(tEnv, "INSERT INTO sink SELECT id, hdrs FROM t");
    assertEquals(
        !org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18"),
        plan.contains("NativeKafkaDecode"),
        plan);
  }

  @Test
  void wholeNotNullHeadersRouteOnEveryLine() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, hdrs MAP<STRING, BYTES> NOT NULL METADATA FROM 'headers'", "json"));
    tEnv.executeSql(
        "CREATE TABLE sink (id BIGINT, hdrs MAP<STRING, BYTES> NOT NULL)"
            + " WITH ('connector' = 'blackhole')");
    String plan = NativePlanner.explain(tEnv, "INSERT INTO sink SELECT id, hdrs FROM t");
    assertTrue(plan.contains("NativeKafkaDecode"), plan);
  }

  @Test
  void keyedTableWithMetadataKeepsTheScanOnFlink() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("k STRING, id BIGINT, off BIGINT METADATA FROM 'offset'", "json")
            .replace(
                "'format' = 'json')",
                "'key.format' = 'raw', 'key.fields' = 'k', 'value.format' = 'json',"
                    + " 'value.fields-include' = 'EXCEPT_KEY')"));
    assertStaysOnFlink(tEnv, "SELECT k, id, off FROM t");
  }

  @Test
  void metadataRowtimeKeepsItsWatermarkNatively() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table(
            "id BIGINT, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',"
                + " WATERMARK FOR ts AS ts - INTERVAL '1' SECOND",
            "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, ts FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode") && plan.contains("watermarkColumn=[ts]"),
        "a TIMESTAMP_LTZ(3) metadata rowtime should keep its native watermark:\n" + plan);
  }

  @Test
  void zoneDependentMetadataRowtimeKeepsTheScanOnFlink() {
    // A TIMESTAMP(3) rowtime reads the record timestamp through the session time zone.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table(
            "id BIGINT, ts TIMESTAMP(3) METADATA FROM 'timestamp', WATERMARK FOR ts AS ts",
            "json"));
    assertStaysOnFlink(tEnv, "SELECT id, ts FROM t");
  }

  @Test
  void scansReadingDifferentMetadataDoNotShareADecode() {
    // Same topic, options and column name and type, but different metadata keys.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, m STRING METADATA FROM 'topic'", "json"));
    tEnv.executeSql(
        table("id BIGINT, m STRING METADATA FROM 'timestamp-type'", "json")
            .replace("CREATE TABLE t ", "CREATE TABLE u "));
    String plan = NativePlanner.explain(tEnv, "SELECT id, m FROM t UNION ALL SELECT id, m FROM u");
    assertTrue(
        plan.contains("metadata=[{m=topic}]") && plan.contains("metadata=[{m=timestamp-type}]"),
        "each table should decode its own metadata:\n" + plan);
  }

  @Test
  void timeAndVarbinaryColumnsRoute() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, t TIME(0), t3 TIME(3), b VARBINARY(1024)", "json"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, t, t3, b FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
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
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.JSON_PARSER_OPTION,
        "Flink 1.18 omits json.decode.json-parser.enabled from its factory option whitelist");
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
  void nestedMaxwellSchemaRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table(
            "id BIGINT, info ROW<name STRING, score DOUBLE>, tags ARRAY<STRING>,"
                + " attrs MAP<STRING, BIGINT>, bag MULTISET<STRING>,"
                + " items ARRAY<ROW<id BIGINT, name STRING>>",
            "maxwell-json"));
    String plan = NativePlanner.explain(tEnv, "SELECT * FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a nested Maxwell table should decode natively:\n" + plan);
  }

  @Test
  void nestedCanalSchemaRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table(
            "id BIGINT, info ROW<name STRING, score DOUBLE>, tags ARRAY<STRING>,"
                + " attrs MAP<STRING, BIGINT>, bag MULTISET<STRING>",
            "canal-json"));
    String plan = NativePlanner.explain(tEnv, "SELECT * FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a nested Canal table should decode natively:\n" + plan);
  }

  @Test
  void maxwellSchemaWiderThan128ColumnsRoutes() {
    StringBuilder columns = new StringBuilder("c0 BIGINT");
    for (int i = 1; i < 200; i++) {
      columns.append(", c").append(i).append(" BIGINT");
    }
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table(columns.toString(), "maxwell-json"));
    String plan = NativePlanner.explain(tEnv, "SELECT * FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a 200-column Maxwell table should decode natively:\n" + plan);
  }

  @Test
  void nestedCanalUnsupportedLeafKeepsTheScanOnFlink() {
    // Nesting no longer gates Maxwell/Canal, but the JSON type gate still recurses into it.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(table("id BIGINT, info ROW<name STRING, bin BINARY(4)>", "canal-json"));
    assertStaysOnFlink(tEnv, "SELECT * FROM t");
  }

  @Test
  void canalIncludeFiltersRoute() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "canal-json")
            .replace(
                "'canal-json')",
                "'canal-json', 'canal-json.database.include' = 'shop',"
                    + " 'canal-json.table.include' = 'order_\\d+|users')"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a Canal table with include filters should decode natively:\n" + plan);
  }

  @Test
  void invalidCanalIncludePatternFailsWithFlinksOwnError() {
    // Flink compiles the pattern when it builds the scan's deserializer and fails planning, so the
    // native path must leave the scan to Flink rather than replace that failure with its own.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "canal-json")
            .replace("'canal-json')", "'canal-json', 'canal-json.table.include' = 'orders[')"));
    PatternSyntaxException error =
        assertThrows(
            PatternSyntaxException.class,
            () -> NativePlanner.explain(tEnv, "SELECT id, name FROM t"));
    assertTrue(
        Arrays.stream(error.getStackTrace())
            .anyMatch(
                frame ->
                    frame
                        .getClassName()
                        .equals(
                            "org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema")),
        "the pattern error should come from Flink's own Canal deserializer");
  }

  @Test
  void debeziumAvroConfluentRoutes() {
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "debezium-avro-confluent")
            .replace(
                "'debezium-avro-confluent')",
                "'debezium-avro-confluent', 'debezium-avro-confluent.url' ="
                    + " 'http://localhost:8081')"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "a plain-URL debezium-avro-confluent table should decode natively:\n" + plan);
  }

  @Test
  void debeziumAvroConfluentUserInfoAuthRoutes() {
    // The header-only auth schemes (USER_INFO basic, STATIC_TOKEN bearer) translate to the exact
    // Authorization header the Confluent client sends, so a secured registry stays native.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "debezium-avro-confluent")
            .replace(
                "'debezium-avro-confluent')",
                "'debezium-avro-confluent', 'debezium-avro-confluent.url' ="
                    + " 'http://localhost:8081',"
                    + " 'debezium-avro-confluent.basic-auth.credentials-source' = 'USER_INFO',"
                    + " 'debezium-avro-confluent.basic-auth.user-info' = 'user:pw')"));
    String plan = NativePlanner.explain(tEnv, "SELECT id, name FROM t");
    assertTrue(
        plan.contains("NativeKafkaDecode"),
        "USER_INFO registry auth should decode natively:\n" + plan);
  }

  @Test
  void debeziumAvroConfluentUntranslatedRegistryAuthKeepsTheScanOnFlink() {
    // Registry SSL/client-properties options and the credential sources beyond the header-only
    // pair aren't translated to the plain-HTTP native fetch — the same fallback set as
    // avro-confluent.
    StreamTableEnvironment tEnv = env();
    tEnv.executeSql(
        table("id BIGINT, name STRING", "debezium-avro-confluent")
            .replace(
                "'debezium-avro-confluent')",
                "'debezium-avro-confluent', 'debezium-avro-confluent.url' ="
                    + " 'http://localhost:8081',"
                    + " 'debezium-avro-confluent.basic-auth.credentials-source' = 'URL')"));
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
        plan.contains("NativeKafkaDecode"),
        "a supported schema should route natively:\n" + plan);
  }

  private static void assertStaysOnFlink(StreamTableEnvironment tEnv, String sql) {
    String plan = NativePlanner.explain(tEnv, sql);
    assertTrue(
        !plan.contains("NativeKafkaDecode"),
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
