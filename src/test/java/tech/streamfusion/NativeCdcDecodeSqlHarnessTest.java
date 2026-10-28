package tech.streamfusion;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end tests for the CDC native-decode path (Phase 3): a {@code CREATE TABLE ...
 * 'connector'='kafka', 'format'='<cdc-json>'} either routes through the planner to {@link
 * tech.streamfusion.planner.StreamPhysicalNativeKafkaDecode} — Flink's own KafkaSource
 * consumes the raw bytes and the native {@code MessageDecoder} turns each envelope into physical rows
 * carrying their {@code RowKind} on {@code $row_kind$} — or, where we can't reproduce Flink exactly,
 * cleanly falls back to Flink's own decoder.
 *
 * <ul>
 *   <li><b>Debezium / OGG</b> (full pre/post images): route natively. {@link
 *       NativeParity#assertChangelogParity} runs the query on stock Flink and on the native decode and
 *       asserts the collapsed changelogs match — exact CDC-semantics parity.
 *   <li><b>Maxwell / Canal</b> (post-image + partial {@code old}): route natively too. Their
 *       UPDATE_BEFORE follows Flink's findValue KEY-presence rule — an explicit null in {@code old}
 *       stays null, an absent key copies {@code data} — reproduced by a native per-message key scan
 *       of the raw {@code old} JSON, so the changelog is exact (the per-scenario envelope is pinned
 *       by the container-free {@code CdcDecodeParityTest}).
 * </ul>
 *
 * <p>Each input sequence collapses to the same materialized result — only id=1 (name "a2") survives.
 * Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka); bounded ({@code latest-offset})
 * scans so the runs terminate.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeCdcDecodeSqlHarnessTest {

  @Test
  void allFourCdcDialectsRouteNativelyWithParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();

      // Debezium: nested before/after, op c/u/d — routes natively, exact parity.
      assertParity(
          brokers,
          "cdc-debezium",
          "debezium-json",
          List.of(
              "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op\":\"c\"}",
              "{\"before\":null,\"after\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"op\":\"c\"}",
              "{\"before\":{\"id\":1,\"name\":\"a\",\"score\":1.5},"
                  + "\"after\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"op\":\"u\"}",
              "{\"before\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"after\":null,\"op\":\"d\"}"));

      // OGG: same nested layout, op_type I/U/D — routes natively, exact parity.
      assertParity(
          brokers,
          "cdc-ogg",
          "ogg-json",
          List.of(
              "{\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op_type\":\"I\"}",
              "{\"after\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"op_type\":\"I\"}",
              "{\"before\":{\"id\":1,\"name\":\"a\",\"score\":1.5},"
                  + "\"after\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"op_type\":\"U\"}",
              "{\"before\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"after\":null,\"op_type\":\"D\"}"));

      // Debezium with a nested ROW column — exercises nested envelope decode + the per-field gather
      // (interleave over a nested struct child) through the changelog.
      assertParity(
          brokers,
          "cdc-nested",
          "debezium-json",
          "id BIGINT, info ROW<x BIGINT, y STRING>",
          List.of(
              "{\"before\":null,\"after\":{\"id\":1,\"info\":{\"x\":10,\"y\":\"a\"}},\"op\":\"c\"}",
              "{\"before\":null,\"after\":{\"id\":2,\"info\":{\"x\":30,\"y\":\"c\"}},\"op\":\"c\"}",
              "{\"before\":{\"id\":1,\"info\":{\"x\":10,\"y\":\"a\"}},"
                  + "\"after\":{\"id\":1,\"info\":{\"x\":20,\"y\":\"b\"}},\"op\":\"u\"}",
              "{\"before\":{\"id\":2,\"info\":{\"x\":30,\"y\":\"c\"}},\"after\":null,\"op\":\"d\"}"));

      // Debezium with ignore-parse-errors: the native decode skips an undecodable message (malformed
      // JSON, an unknown op) exactly as Flink's catch-everything-per-message skip does, so the table
      // still routes natively and the surviving changelog matches.
      produce(
          brokers,
          "cdc-skip",
          List.of(
              "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op\":\"c\"}",
              "{\"before\":null,\"after\":{\"id\":2,", // malformed: skipped by both
              "{\"before\":null,\"after\":{\"id\":3,\"name\":\"x\",\"score\":3.5},\"op\":\"x\"}",
              "{\"before\":{\"id\":1,\"name\":\"a\",\"score\":1.5},"
                  + "\"after\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"op\":\"u\"}"));
      NativeParity.assertChangelogParity(
          environment(
              brokers,
              "cdc-skip",
              "debezium-json",
              SCALAR_COLUMNS,
              ", 'debezium-json.ignore-parse-errors' = 'true'"),
          "SELECT * FROM cdc");

      // Maxwell: partial `old` merge via the key-presence rule — routes natively, exact parity.
      // The second update carries an explicit null in `old` (name changed FROM null): presence
      // keeps the null where the pre-presence decode would have copied `data`.
      assertParity(
          brokers,
          "cdc-maxwell",
          "maxwell-json",
          List.of(
              "{\"data\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"type\":\"insert\"}",
              "{\"data\":{\"id\":2,\"name\":null,\"score\":2.5},\"type\":\"insert\"}",
              "{\"data\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"old\":{\"name\":\"a\"},\"type\":\"update\"}",
              "{\"data\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"old\":{\"name\":null},\"type\":\"update\"}",
              "{\"data\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"type\":\"delete\"}"));

      // Canal: data/old arrays fanning out per element, same presence rule — routes natively.
      assertParity(
          brokers,
          "cdc-canal",
          "canal-json",
          List.of(
              "{\"data\":[{\"id\":1,\"name\":\"a\",\"score\":1.5},"
                  + "{\"id\":2,\"name\":\"b\",\"score\":2.5}],\"type\":\"INSERT\"}",
              "{\"data\":[{\"id\":1,\"name\":\"a2\",\"score\":1.5}],"
                  + "\"old\":[{\"name\":\"a\"}],\"type\":\"UPDATE\"}",
              "{\"data\":[{\"id\":2,\"name\":\"b\",\"score\":2.5}],\"type\":\"DELETE\"}"));
    }
  }

  private static final String SCALAR_COLUMNS = "id BIGINT, name STRING, score DOUBLE";

  private static final String NESTED_COLUMNS =
      "id BIGINT, name STRING, info ROW<name STRING, score DOUBLE>, tags ARRAY<STRING>,"
          + " attrs MAP<STRING, BIGINT>, bag MULTISET<STRING>,"
          + " items ARRAY<ROW<id BIGINT, name STRING>>";

  /**
   * Nested and wide Maxwell/Canal tables route natively with the exact changelog, RowKinds
   * included: a column name found inside a nested {@code old} value keeps that UPDATE_BEFORE field
   * NULL, presence spans Canal's whole {@code old} array, and columns past the 128th coalesce like
   * any other. Map entry order is pinned message by message by {@code CdcDecodeParityTest}.
   */
  @Test
  void nestedAndWideMaxwellAndCanalRouteNativelyWithParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      StringBuilder manyKeys = new StringBuilder("{");
      for (int i = 0; i < 40; i++) {
        manyKeys.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":").append(i);
      }
      manyKeys.append('}');
      String first =
          "{\"id\":1,\"name\":\"a\",\"info\":{\"name\":\"n1\",\"score\":1.5},\"tags\":[\"t1\"],"
              + "\"attrs\":" + manyKeys + ",\"bag\":{\"b\":1},\"items\":[{\"id\":9,\"name\":\"i\"}]}";
      String firstUpdated =
          "{\"id\":1,\"name\":\"a\",\"info\":{\"name\":\"n2\",\"score\":1.5},\"tags\":[\"t1\"],"
              + "\"attrs\":{\"x\":1,\"id\":2},\"bag\":{\"b\":1},\"items\":[]}";
      String second =
          "{\"id\":2,\"name\":\"b\",\"info\":null,\"tags\":[],\"attrs\":{},\"bag\":null,"
              + "\"items\":null}";
      produce(
          brokers,
          "cdc-maxwell-nested",
          List.of(
              "{\"data\":" + first + ",\"type\":\"insert\"}",
              "{\"data\":" + second + ",\"type\":\"insert\"}",
              "{\"data\":" + firstUpdated + ",\"old\":{\"info\":{\"name\":\"n1\"},"
                  + "\"attrs\":" + manyKeys + ",\"items\":[{\"id\":9,\"name\":\"i\"}]},"
                  + "\"type\":\"update\"}",
              "{\"data\":" + second + ",\"type\":\"delete\"}"));
      NativeParity.assertKindedParity(
          environment(brokers, "cdc-maxwell-nested", "maxwell-json", NESTED_COLUMNS),
          "SELECT * FROM cdc");

      produce(
          brokers,
          "cdc-canal-nested",
          List.of(
              "{\"data\":[" + first + "," + second + "],\"type\":\"INSERT\"}",
              "{\"data\":[" + firstUpdated + "," + second + "],"
                  + "\"old\":[{\"info\":{\"name\":\"n1\"}},{\"bag\":{\"id\":1}}],"
                  + "\"type\":\"UPDATE\"}",
              "{\"data\":[" + second + "],\"type\":\"DELETE\"}"));
      NativeParity.assertKindedParity(
          environment(brokers, "cdc-canal-nested", "canal-json", NESTED_COLUMNS),
          "SELECT * FROM cdc");

      StringBuilder wideColumns = new StringBuilder();
      StringBuilder wideRow = new StringBuilder("{");
      for (int i = 0; i < 140; i++) {
        wideColumns.append(i == 0 ? "" : ", ").append("c").append(i).append(" BIGINT");
        wideRow.append(i == 0 ? "" : ",").append("\"c").append(i).append("\":").append(i);
      }
      wideRow.append('}');
      produce(
          brokers,
          "cdc-maxwell-wide",
          List.of(
              "{\"data\":" + wideRow + ",\"type\":\"insert\"}",
              "{\"data\":" + wideRow + ",\"old\":{\"c2\":-2,\"c130\":-130,\"c139\":null},"
                  + "\"type\":\"update\"}"));
      NativeParity.assertKindedParity(
          environment(brokers, "cdc-maxwell-wide", "maxwell-json", wideColumns.toString()),
          "SELECT * FROM cdc");
    }
  }

  private static void assertParity(
      String brokers, String topic, String format, List<String> messages) throws Exception {
    assertParity(brokers, topic, format, SCALAR_COLUMNS, messages);
  }

  private static void assertParity(
      String brokers, String topic, String format, String columns, List<String> messages)
      throws Exception {
    produce(brokers, topic, messages);
    NativeParity.assertChangelogParity(
        environment(brokers, topic, format, columns), "SELECT * FROM cdc");
  }


  private static Supplier<TableEnvironment> environment(
      String brokers, String topic, String format, String columns) {
    return environment(brokers, topic, format, columns, "");
  }

  private static Supplier<TableEnvironment> environment(
      String brokers, String topic, String format, String columns, String extraOptions) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(cdcTable(brokers, topic, format, columns, extraOptions));
      return tEnv;
    };
  }

  private static String cdcTable(
      String brokers, String topic, String format, String columns, String extraOptions) {
    return "CREATE TABLE cdc ("
        + columns
        + ") WITH ("
        + "'connector' = 'kafka', "
        + "'topic' = '"
        + topic
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = '"
        + topic
        + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
        + "'format' = '"
        + format
        + "'"
        + extraOptions
        + ")";
  }

  private static void produce(String brokers, String topic, List<String> messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (String message : messages) {
        producer.send(new ProducerRecord<>(topic, 0, null, message.getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
  }
}
