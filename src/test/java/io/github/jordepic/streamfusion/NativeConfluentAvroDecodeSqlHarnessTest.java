package io.github.jordepic.streamfusion;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end parity tests for the {@code avro-confluent} native-decode path: a Confluent-framed Kafka
 * table routes to the native decode operator, which fetches each frame's writer schema from the schema
 * registry by id on first sight and resolves it against the reader schema derived from the table's
 * {@code RowType} — exactly the lookup-and-resolve Flink's own {@code avro-confluent} deserializer
 * performs, compared here against it byte for byte.
 *
 * <p>The registry is a local HTTP server implementing the one read endpoint both sides use
 * ({@code GET /schemas/ids/<id>}); Flink's baseline talks to it through the real Confluent client. The
 * topic carries two writer-schema versions with producer-style record names ({@code com.example.*} —
 * exercising the alias patch that reproduces Avro Java's lenient name check), reordered fields, an
 * extra field the reader drops, and a mid-stream schema switch. Opt-in via {@code SF_BENCHMARK=true}.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeConfluentAvroDecodeSqlHarnessTest {

  private static final int MESSAGES = 2_000;

  private static final Schema WRITER_V1 =
      SchemaBuilder.record("User")
          .namespace("com.example")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .optionalDouble("score")
          .requiredString("extra")
          .endRecord();

  // Evolved: fields reordered and the extra field dropped; resolution matches by name.
  private static final Schema WRITER_V2 =
      SchemaBuilder.record("UserV2")
          .namespace("com.example")
          .fields()
          .requiredString("name")
          .optionalDouble("score")
          .requiredLong("id")
          .endRecord();

  private static HttpServer registry;

  @BeforeAll
  static void startRegistry() throws Exception {
    Map<Integer, Schema> schemas = Map.of(7, WRITER_V1, 9, WRITER_V2);
    registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    registry.createContext(
        "/schemas/ids/",
        exchange -> {
          int id = Integer.parseInt(exchange.getRequestURI().getPath().substring("/schemas/ids/".length()));
          Schema schema = schemas.get(id);
          if (schema == null) {
            exchange.sendResponseHeaders(404, -1);
            return;
          }
          // The registry envelope: the schema JSON as an escaped string field.
          String quoted = schema.toString().replace("\\", "\\\\").replace("\"", "\\\"");
          byte[] body = ("{\"schema\":\"" + quoted + "\"}").getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    registry.start();
  }

  @AfterAll
  static void stopRegistry() {
    registry.stop(0);
  }

  @Test
  void confluentAvroDecodesNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      produce(kafka.getBootstrapServers(), "confluent-avro");

      // The full read, spanning the mid-stream writer-schema switch.
      NativeParity.assertParity(
          environment(kafka.getBootstrapServers(), "confluent-avro"), "SELECT * FROM t");

      // A projected read: the planner pushes the projection into the decode, narrowing the reader
      // schema — the writers' unread fields are resolved away, never built into Arrow.
      NativeParity.assertParity(
          environment(kafka.getBootstrapServers(), "confluent-avro"),
          "SELECT name, score FROM t WHERE id > 100");
    }
  }

  private static void produce(String brokers, String topic) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      List<byte[]> values = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        // First half under writer v1 (id 7), second half under v2 (id 9) — one topic, evolving schema.
        boolean v1 = i < MESSAGES / 2;
        Schema schema = v1 ? WRITER_V1 : WRITER_V2;
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", (long) i);
        record.put("name", "row-" + i);
        record.put("score", i % 3 == 0 ? null : i + 0.5);
        if (v1) {
          record.put("extra", "dropped-" + i);
        }
        values.add(framed(v1 ? 7 : 9, schema, record));
      }
      for (byte[] value : values) {
        producer.send(new ProducerRecord<>(topic, 0, null, value));
      }
      producer.flush();
    }
  }

  /** The Confluent wire format: magic {@code 0x00} + 4-byte BE schema id + the Avro binary datum. */
  private static byte[] framed(int schemaId, Schema schema, GenericRecord record) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static Supplier<TableEnvironment> environment(String brokers, String topic) {
    String registryUrl = "http://localhost:" + registry.getAddress().getPort();
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(
          "CREATE TABLE t (id BIGINT, name STRING, score DOUBLE) WITH ('connector' = 'kafka', "
              + "'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = '"
              + topic
              + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
              + "'format' = 'avro-confluent', 'avro-confluent.url' = '"
              + registryUrl
              + "')");
      return tEnv;
    };
  }
}
