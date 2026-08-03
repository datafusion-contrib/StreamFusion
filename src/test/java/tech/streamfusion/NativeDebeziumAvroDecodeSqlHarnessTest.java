package tech.streamfusion;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
 * End-to-end parity for the {@code debezium-avro-confluent} native-decode path: a Kafka table with
 * the Debezium Avro envelope routes to the native decode operator, which fetches each frame's
 * writer schema from the registry by id, aligns it onto the reader derived from the envelope row
 * type, and fans the envelope out to changelog rows — compared against stock Flink's own decoder
 * over the identical topic. The writers carry Debezium's real shape (one {@code Value} record
 * referenced by both images, {@code source}/{@code ts_ms} extras) across two schema versions, plus
 * a tombstone. Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeDebeziumAvroDecodeSqlHarnessTest {

  private static final Schema VALUE_V1 =
      SchemaBuilder.record("Value")
          .namespace("dbserver1.inventory.customers")
          .fields()
          .optionalLong("id")
          .optionalString("name")
          .optionalDouble("score")
          .optionalString("internal")
          .endRecord();

  private static final Schema ENVELOPE_V1 = envelope("Envelope", VALUE_V1);

  // Evolved: the internal field dropped, the rest reordered; resolution matches by name.
  private static final Schema VALUE_V2 =
      SchemaBuilder.record("ValueV2")
          .namespace("dbserver1.inventory.customers")
          .fields()
          .optionalString("name")
          .optionalDouble("score")
          .optionalLong("id")
          .endRecord();

  private static final Schema ENVELOPE_V2 = envelope("EnvelopeV2", VALUE_V2);

  private static HttpServer registry;

  @BeforeAll
  static void startRegistry() throws Exception {
    Map<Integer, Schema> schemas = Map.of(7, ENVELOPE_V1, 9, ENVELOPE_V2);
    registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    registry.createContext(
        "/schemas/ids/",
        exchange -> {
          int id =
              Integer.parseInt(
                  exchange.getRequestURI().getPath().substring("/schemas/ids/".length()));
          Schema schema = schemas.get(id);
          if (schema == null) {
            exchange.sendResponseHeaders(404, -1);
            return;
          }
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
  void debeziumAvroRoutesNativelyWithChangelogParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String topic = "cdc-debezium-avro";
      produce(kafka.getBootstrapServers(), topic);
      NativeParity.assertChangelogParity(
          environment(kafka.getBootstrapServers(), topic), "SELECT * FROM cdc");
    }
  }

  private static void produce(String brokers, String topic) throws Exception {
    List<byte[]> messages = new ArrayList<>();
    messages.add(message(7, ENVELOPE_V1, null, image(VALUE_V1, 1L, "a", 1.5), "c"));
    messages.add(message(7, ENVELOPE_V1, null, image(VALUE_V1, 2L, "b", 2.5), "r"));
    messages.add(null); // tombstone, skipped by both engines
    messages.add(
        message(7, ENVELOPE_V1, image(VALUE_V1, 1L, "a", 1.5), image(VALUE_V1, 1L, "a2", 1.5), "u"));
    // The evolved writer takes over mid-topic.
    messages.add(
        message(9, ENVELOPE_V2, image(VALUE_V2, 2L, "b", 2.5), image(VALUE_V2, 2L, "b2", null), "u"));
    messages.add(message(9, ENVELOPE_V2, image(VALUE_V2, 1L, "a2", 1.5), null, "d"));
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (byte[] message : messages) {
        producer.send(new ProducerRecord<>(topic, 0, null, message));
      }
      producer.flush();
    }
  }

  private static Schema envelope(String name, Schema value) {
    return SchemaBuilder.record(name)
        .namespace(value.getNamespace())
        .fields()
        .name("before")
        .type()
        .unionOf()
        .nullType()
        .and()
        .type(value)
        .endUnion()
        .nullDefault()
        .name("after")
        .type()
        .unionOf()
        .nullType()
        .and()
        .type(value)
        .endUnion()
        .nullDefault()
        .optionalString("source")
        .requiredString("op")
        .optionalLong("ts_ms")
        .endRecord();
  }

  private static GenericRecord image(Schema valueSchema, Long id, String name, Double score) {
    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("id", id);
    record.put("name", name);
    record.put("score", score);
    if (valueSchema.getField("internal") != null) {
      record.put("internal", "writer-only");
    }
    return record;
  }

  private static byte[] message(
      int schemaId, Schema envelopeSchema, GenericRecord before, GenericRecord after, String op)
      throws Exception {
    GenericRecord envelope = new GenericData.Record(envelopeSchema);
    envelope.put("before", before);
    envelope.put("after", after);
    envelope.put("source", "src");
    envelope.put("op", op);
    envelope.put("ts_ms", 42L);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(envelopeSchema).write(envelope, encoder);
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
          "CREATE TABLE cdc (id BIGINT, name STRING, score DOUBLE) WITH ('connector' = 'kafka', "
              + "'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = '"
              + topic
              + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
              + "'format' = 'debezium-avro-confluent', 'debezium-avro-confluent.url' = '"
              + registryUrl
              + "')");
      return tEnv;
    };
  }
}
