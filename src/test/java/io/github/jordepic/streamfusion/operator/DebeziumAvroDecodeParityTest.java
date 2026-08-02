package io.github.jordepic.streamfusion.operator;

import com.sun.net.httpserver.HttpServer;
import io.github.jordepic.streamfusion.format.avroconfluent.DebeziumAvroConfluentFormatProvider;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Pins the native {@code debezium-avro-confluent} decode to Flink's own {@link
 * DebeziumAvroDeserializationSchema}, message by message: both engines fetch each frame's writer
 * schema from the same local registry (the one read endpoint, {@code GET /schemas/ids/<id>}) and
 * must produce identical changelogs — RowKinds included — or fail together. The writer schemas
 * carry the real Debezium shape the alignment machinery exists for: producer record names, one
 * {@code Value} record referenced by both {@code before} and {@code after}, envelope fields
 * ({@code source}, {@code ts_ms}) and an image field the reader resolves away, plus a second
 * evolved writer version resolved mid-topic.
 */
@Tag("streamfusion-avro")
class DebeziumAvroDecodeParityTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new DoubleType(),
            new TimestampType(3)
          },
          new String[] {"id", "name", "score", "ts"});

  private static final Schema VALUE_V1 =
      SchemaBuilder.record("Value")
          .namespace("dbserver1.inventory.customers")
          .fields()
          .optionalLong("id")
          .optionalString("name")
          .optionalDouble("score")
          .name("internal")
          .type()
          .optional()
          .stringType()
          .name("ts")
          .type()
          .optional()
          .type(timestampMillis())
          .endRecord();

  private static final Schema ENVELOPE_V1 = envelope("Envelope", VALUE_V1);

  // Evolved: the score column dropped and the remaining fields reordered; both engines resolve the
  // reader's score to its null default.
  private static final Schema VALUE_V2 =
      SchemaBuilder.record("ValueV2")
          .namespace("dbserver2.inventory.customers")
          .fields()
          .optionalString("name")
          .name("ts")
          .type()
          .optional()
          .type(timestampMillis())
          .name("id")
          .type()
          .optional()
          .longType()
          .endRecord();

  private static final Schema ENVELOPE_V2 = envelope("EnvelopeV2", VALUE_V2);

  private static final int V1_ID = 7;
  private static final int V2_ID = 9;

  private static HttpServer registry;

  @BeforeAll
  static void startRegistry() throws Exception {
    Map<Integer, Schema> schemas = Map.of(V1_ID, ENVELOPE_V1, V2_ID, ENVELOPE_V2);
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
  void opsFanOutLikeFlink() throws Exception {
    assertParity("insert", message(V1_ID, null, image(1L, "a", 1.5, 1_000L), "c"));
    assertParity("snapshot read", message(V1_ID, null, image(2L, "b", 2.5, 2_000L), "r"));
    assertParity(
        "update",
        message(V1_ID, image(1L, "a", 1.5, 1_000L), image(1L, "a2", 1.5, 3_000L), "u"));
    assertParity("delete", message(V1_ID, image(1L, "a2", 1.5, 3_000L), null, "d"));
    assertParity(
        "null field values inside the images",
        message(V1_ID, image(1L, null, null, null), image(1L, "x", null, null), "u"));
  }

  @Test
  void corruptMessagesFailOnBothEngines() throws Exception {
    assertParity("update without before", message(V1_ID, null, image(1L, "a", 1.5, 1_000L), "u"));
    assertParity("delete without before", message(V1_ID, null, image(1L, "a", 1.5, 1_000L), "d"));
    assertParity("insert without after", message(V1_ID, image(1L, "a", 1.5, 1_000L), null, "c"));
    assertParity("unknown op", message(V1_ID, null, image(1L, "a", 1.5, 1_000L), "t"));
    assertParity("truncated frame", new byte[] {0, 0, 0, 0, V1_ID, 2});
  }

  @Test
  void tombstonesAreSkippedOnBothEngines() throws Exception {
    assertParity("empty message", new byte[0]);
    assertParity("null message", null);
  }

  @Test
  void bytesAfterTheFirstFrameAreIgnoredLikeFlink() throws Exception {
    // Flink reads exactly one envelope per message and never checks the remaining buffer:
    // trailing junk after a complete frame is ignored, and a second concatenated frame is dead
    // bytes — the changelog comes from the first envelope alone.
    byte[] first = message(V1_ID, null, image(1L, "a", 1.5, 1_000L), "c");
    byte[] second = message(V1_ID, null, image(2L, "b", 2.5, 2_000L), "c");
    assertParity("frame with trailing junk", concat(first, new byte[] {(byte) 0xFF, 0x01}));
    assertParity("two concatenated frames", concat(first, second));
  }

  private static byte[] concat(byte[] head, byte[] tail) {
    ByteArrayOutputStream whole = new ByteArrayOutputStream();
    whole.writeBytes(head);
    whole.writeBytes(tail);
    return whole.toByteArray();
  }

  @Test
  void evolvedWriterSchemaResolvesLikeFlink() throws Exception {
    GenericRecord before = new GenericData.Record(VALUE_V2);
    before.put("id", 5L);
    before.put("name", "v2");
    before.put("ts", 4_000L);
    GenericRecord after = new GenericData.Record(VALUE_V2);
    after.put("id", 5L);
    after.put("name", "v2b");
    after.put("ts", 5_000L);
    GenericRecord envelope = new GenericData.Record(ENVELOPE_V2);
    envelope.put("before", before);
    envelope.put("after", after);
    envelope.put("source", "src");
    envelope.put("op", "u");
    envelope.put("ts_ms", 5L);
    assertParity("evolved writer", framed(V2_ID, ENVELOPE_V2, envelope));
  }

  private static Schema timestampMillis() {
    return new Schema.Parser()
        .parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
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

  private static Consumer<GenericRecord> image(Long id, String name, Double score, Long tsMillis) {
    return record -> {
      record.put("id", id);
      record.put("name", name);
      record.put("score", score);
      record.put("internal", "writer-only");
      record.put("ts", tsMillis);
    };
  }

  private static byte[] message(
      int schemaId, Consumer<GenericRecord> before, Consumer<GenericRecord> after, String op)
      throws Exception {
    GenericRecord envelope = new GenericData.Record(ENVELOPE_V1);
    if (before != null) {
      GenericRecord record = new GenericData.Record(VALUE_V1);
      before.accept(record);
      envelope.put("before", record);
    }
    if (after != null) {
      GenericRecord record = new GenericData.Record(VALUE_V1);
      after.accept(record);
      envelope.put("after", record);
    }
    envelope.put("source", "src");
    envelope.put("op", op);
    envelope.put("ts_ms", 42L);
    return framed(schemaId, ENVELOPE_V1, envelope);
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

  private static void assertParity(String label, byte[] message) {
    DecodeParityHarness harness = new DecodeParityHarness(ROW_TYPE, true);
    String url = "http://localhost:" + registry.getAddress().getPort();
    harness.assertParity(
        label,
        () -> flinkDecode(harness, message, url),
        () ->
            harness.nativeDecode(
                new DebeziumAvroConfluentFormatProvider(),
                message,
                Map.of("format", "debezium-avro-confluent", "debezium-avro-confluent.url", url),
                false));
  }

  private static List<List<Object>> flinkDecode(
      DecodeParityHarness harness, byte[] message, String registryUrl) throws Exception {
    DebeziumAvroDeserializationSchema schema =
        new DebeziumAvroDeserializationSchema(
            ROW_TYPE, InternalTypeInfo.of(ROW_TYPE), registryUrl, null, null);
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(
                DebeziumAvroDecodeParityTest.class.getClassLoader());
          }
        });
    List<List<Object>> rows = new ArrayList<>();
    schema.deserialize(
        message,
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            rows.add(harness.fields(row));
          }

          @Override
          public void close() {}
        });
    return rows;
  }
}
