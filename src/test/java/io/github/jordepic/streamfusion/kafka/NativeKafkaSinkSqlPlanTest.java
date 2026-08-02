package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ValidationException;
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

  /** The probe must admit every encode arm the connector library was built with. */
  @Test
  void plansRawSingleColumnSinksNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (name STRING NOT NULL) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (name STRING NOT NULL) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'raw')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  @Test
  void plansProtobufSinksNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING, score DOUBLE) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, name STRING, score DOUBLE) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'protobuf', "
            + "'protobuf.message-class-name' = 'io.github.jordepic.streamfusion.proto.Row')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /** The CDC Avro envelope admits changelog input; registration happens at open, not plan time. */
  @Test
  void plansDebeziumAvroConfluentChangelogSinksNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (name STRING) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (name STRING, cnt BIGINT) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'debezium-avro-confluent', "
            + "'debezium-avro-confluent.url' = 'http://registry:8081')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT name, COUNT(*) FROM src GROUP BY name",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  @Test
  void plansNestedRowsAndArraysNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, items ARRAY<INT>, "
            + "nested ROW<a INT, ts TIMESTAMP_LTZ(3), inner_items ARRAY<ROW<b STRING>>>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, items ARRAY<INT>, "
            + "nested ROW<a INT, ts TIMESTAMP_LTZ(3), inner_items ARRAY<ROW<b STRING>>>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  @Test
  void plansStringKeyedMapsAndMultisetsNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, counts MAP<STRING, INT>, bag MULTISET<STRING>, "
            + "deep MAP<STRING, ARRAY<ROW<a INT>>>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, counts MAP<STRING, INT>, bag MULTISET<STRING>, "
            + "deep MAP<STRING, ARRAY<ROW<a INT>>>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json', "
            + "'json.map-null-key.mode' = 'LITERAL', "
            + "'json.map-null-key.literal' = 'absent')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /**
   * Flink's own JSON converter rejects a non-string map key when the sink translates; the native
   * matcher must decline first so substituting the sink never swallows that rejection.
   */
  @Test
  void keepsFlinksRejectionOfNonStringMapKeys() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, counts MAP<INT, STRING>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, counts MAP<INT, STRING>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    UnsupportedOperationException rejection =
        assertThrows(
            UnsupportedOperationException.class,
            () -> table.explainSql("INSERT INTO output SELECT * FROM src"));

    assertTrue(rejection.getMessage().contains("non-string as key type"), rejection.getMessage());
    assertEquals(0, scan.substitutions());
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("MAP")),
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

  /**
   * A CDC envelope value format is Flink's way of writing a changelog to an ordinary kafka table
   * (the sink requests the full changelog, UPDATE_BEFORE included), so an updating aggregate feeds
   * the native sink directly — the one non-upsert case where changelog input is admitted.
   */
  @Test
  void plansChangelogThroughCdcEnvelopeFormats() {
    for (String format : List.of("debezium-json", "canal-json", "maxwell-json", "ogg-json")) {
      StreamTableEnvironment table = environment();
      table.executeSql(
          "CREATE TABLE src (id BIGINT) "
              + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
      table.executeSql(
          "CREATE TABLE output (id BIGINT, total BIGINT) WITH ("
              + "'connector' = 'kafka', "
              + "'topic' = 'output', "
              + "'properties.bootstrap.servers' = 'broker:9092', "
              + "'format' = '"
              + format
              + "')");

      PhysicalPlanScan scan = NativePlanner.install(table);
      String plan =
          table.explainSql(
              "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
              ExplainDetail.JSON_EXECUTION_PLAN,
              ExplainDetail.CHANGELOG_MODE);

      assertTrue(scan.substitutions() > 0, format + ": " + scan.explainSummary());
      assertTrue(plan.contains("NativeKafkaSink"), format + ": " + plan);
      // The CDC sink requests the full changelog, so the planner must not strip the aggregate's
      // UPDATE_BEFORE rows (an upsert-mode consumer would show I,UA only).
      assertTrue(plan.contains("I,UB,UA"), format + ": " + plan);
    }
  }

  /**
   * Flink's kafka factory allows PRIMARY KEY only alongside a CDC value format, and without a
   * key.format the records still have no key output — the PK must not disturb the native plan.
   */
  @Test
  void plansPrimaryKeyedCdcTableWithoutKeyOutput() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'debezium-json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /** Flink rejects schema-include on a debezium-json sink; the native path must not swallow it. */
  @Test
  void debeziumSchemaIncludeKeepsFlinksRejection() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'debezium-json', "
            + "'debezium-json.schema-include' = 'true')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    ValidationException rejection =
        assertThrows(
            ValidationException.class,
            () -> table.explainSql("INSERT INTO output SELECT * FROM src"));

    assertTrue(messages(rejection).contains("schema-include"), messages(rejection));
    assertEquals(0, scan.substitutions());
  }

  /** upsert-kafka forbids CDC value formats in Flink; the rejection must survive installation. */
  @Test
  void upsertKafkaKeepsFlinksCdcFormatRejection() {
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
            + "'value.format' = 'debezium-json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    ValidationException rejection =
        assertThrows(
            ValidationException.class,
            () ->
                table.explainSql("INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id"));

    assertTrue(messages(rejection).contains("not in insert-only mode"), messages(rejection));
    assertEquals(0, scan.substitutions());
  }

  private static String messages(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append('\n');
    }
    return messages.toString();
  }

  @Test
  void upsertMaterializedSinkKeepsHostSerialization() {
    // When Flink materializes an out-of-order upsert changelog (SinkUpsertMaterializer), the
    // materializer is baked into its sink translation — substituting the sink would drop it, so the
    // matcher must decline. FORCE makes the materialization deterministic for the test.
    StreamTableEnvironment table = environment();
    table.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'upsert-kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'key.format' = 'json', "
            + "'value.format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertFalse(plan.contains("NativeKafkaSink"), plan);
    assertTrue(plan.contains("SinkMaterializer"), plan);
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("upsert-materialized sink")),
        scan::explainSummary);
  }

  @Test
  void plansTheCsvSinkNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING, amount DECIMAL(10, 2), event_day DATE, "
            + "items ARRAY<INT>, nested ROW<a INT, ts TIMESTAMP_LTZ(3)>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, name STRING, amount DECIMAL(10, 2), event_day DATE, "
            + "items ARRAY<INT>, nested ROW<a INT, ts TIMESTAMP_LTZ(3)>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'csv', "
            + "'csv.field-delimiter' = ';', "
            + "'csv.escape-character' = '|', "
            + "'csv.null-literal' = 'N/A', "
            + "'csv.write-bigdecimal-in-scientific-notation' = 'true')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  @Test
  void plansUpsertCsvSerializationNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'upsert-kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'key.format' = 'csv', "
            + "'key.csv.field-delimiter' = ';', "
            + "'value.format' = 'csv')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /**
   * FLOAT/DOUBLE serialize natively on the text formats: the library ports the legacy (JDK ≤ 18)
   * {@code Double.toString} and the runtime probe has confirmed this JVM spells identically.
   */
  @Test
  void csvFloatColumnsSerializeNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, score DOUBLE, ratio FLOAT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, score DOUBLE, ratio FLOAT) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'csv')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  @Test
  void jsonFloatColumnsSerializeNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, score DOUBLE, ratio FLOAT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, score DOUBLE, ratio FLOAT) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /**
   * On a JVM whose {@code Double.toString} is not the legacy spelling (JDK 19+), the probe fails
   * and a FLOAT/DOUBLE column keeps host serialization, with the probe's precise reason. Forced
   * here through the probe's test hook — the suite itself runs on the parity JDK.
   */
  @Test
  void jsonFloatColumnsKeepHostSerializationWhenJvmSpellingDiverges() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, score DOUBLE) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, score DOUBLE) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    JdkFloatSpelling.probeOverride = false;
    try {
      String plan =
          table.explainSql(
              "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);
      assertFalse(plan.contains("NativeKafkaSink"), plan);
      assertTrue(
          scan.fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("jdk float spelling mismatch (JDK 19+)")),
          scan::explainSummary);
    } finally {
      JdkFloatSpelling.probeOverride = null;
    }
  }

  /**
   * A TIME(0) column crosses the Arrow boundary at second granularity, but Flink's CSV converter
   * prints whatever milliseconds the value carries — out-of-contract data would silently
   * truncate, so the matcher declines. SQL DDL resolves every TIME precision to TIME(0) in the
   * sink's physical row type, so in practice a SQL-declared TIME column always stays on the host.
   */
  @Test
  void csvTimeColumnsKeepHostSerialization() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, tod TIME(3)) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, tod TIME(3)) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'csv')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertFalse(plan.contains("NativeKafkaSink"), plan);
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("csv type TIME(0)")),
        scan::explainSummary);
  }

  /**
   * Option combinations Flink's own factory validation refuses must decline BEFORE substitution,
   * so the ValidationException stays Flink's.
   */
  @Test
  void csvQuoteConflictKeepsFlinksValidationError() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'csv', "
            + "'csv.quote-character' = 'X', "
            + "'csv.disable-quote-character' = 'true')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    RuntimeException rejection =
        assertThrows(
            RuntimeException.class,
            () -> table.explainSql("INSERT INTO output SELECT * FROM src"));

    assertTrue(
        rejection.toString().contains("ValidationException")
            || hasValidationCause(rejection),
        rejection.toString());
    assertEquals(0, scan.substitutions());
  }

  private static boolean hasValidationCause(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause.getClass().getSimpleName().equals("ValidationException")) {
        return true;
      }
    }
    return false;
  }

  @Test
  void plansBareAvroSerializationNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING, amount DECIMAL(10, 2), ts TIMESTAMP(3), "
            + "items ARRAY<INT>, counts MAP<STRING, INT>, nested ROW<a INT, b STRING>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, name STRING, amount DECIMAL(10, 2), ts TIMESTAMP(3), "
            + "items ARRAY<INT>, counts MAP<STRING, INT>, nested ROW<a INT, b STRING>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'avro')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /** Registration happens at sink open, so planning an avro-confluent sink needs no registry. */
  @Test
  void plansAvroConfluentSerializationWithoutContactingTheRegistry() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, name STRING) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'avro-confluent', "
            + "'avro-confluent.url' = 'http://registry:8081')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /** Avro is insert-only, so it is a legal upsert key and value format; the key format serializes
   * the PK projection under its own auto-completed {@code <topic>-key} subject. */
  @Test
  void plansUpsertAvroConfluentKeyAndValueFormats() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'upsert-kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'key.format' = 'avro-confluent', "
            + "'key.avro-confluent.url' = 'http://registry:8081', "
            + "'value.format' = 'avro-confluent', "
            + "'value.avro-confluent.url' = 'http://registry:8081')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /**
   * Option and type shapes whose Avro serialization the native path does not reproduce: Avro's
   * JSON encoding, registry auth, TIME(0)'s second-precision boundary, and TIMESTAMP_LTZ under
   * avro-confluent's hard-wired legacy mapping (where Flink itself fails submission).
   */
  @Test
  void keepsUnreproducedAvroShapesOnFlink() {
    assertAvroFallback("(id BIGINT)", "'format' = 'avro', 'avro.encoding' = 'json'");
    assertAvroFallback("(id BIGINT, tod TIME(0))", "'format' = 'avro'");
    assertAvroFallback(
        "(id BIGINT)",
        "'format' = 'avro-confluent', "
            + "'avro-confluent.url' = 'http://registry:8081', "
            + "'avro-confluent.basic-auth.credentials-source' = 'USER_INFO', "
            + "'avro-confluent.basic-auth.user-info' = 'user:pass'");
  }

  private static void assertAvroFallback(String columns, String formatOptions) {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src " + columns + " WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output " + columns + " WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + formatOptions + ")");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertFalse(plan.contains("NativeKafkaSink"), plan);
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("not natively encoded")),
        scan::explainSummary);
  }

  private static StreamTableEnvironment environment() {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    return StreamTableEnvironment.create(environment);
  }
}
