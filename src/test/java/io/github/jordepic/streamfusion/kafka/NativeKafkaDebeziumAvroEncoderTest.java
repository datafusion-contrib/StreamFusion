package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.format.LogicalTypeDescriptors;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Byte-level referee for the {@code debezium-avro-confluent} sink: every changelog row must frame
 * the identical Confluent bytes Flink's {@code DebeziumAvroSerializationSchema} produces — the
 * envelope record (before/after images by row kind, op {@code c}/{@code d}), the derived envelope
 * writer schema, and the id returned by one shared stub registry.
 */
@Tag("streamfusion-kafka")
class NativeKafkaDebeziumAvroEncoderTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
          },
          new String[] {"id", "name", "score"});

  private HttpServer registry;

  @AfterEach
  void stopRegistry() {
    if (registry != null) {
      registry.stop(0);
    }
  }

  @Test
  void matchesFlinkForEveryRowKind() throws Exception {
    String url = stubRegistry();
    DebeziumAvroSerializationSchema flink =
        new DebeziumAvroSerializationSchema(ROW_TYPE, url, "t-value", null);
    flink.open(null);

    List<RowData> rows = new ArrayList<>();
    for (RowKind kind : RowKind.values()) {
      GenericRowData full =
          GenericRowData.of(7L, StringData.fromString("row-" + kind.shortString()), 2.5);
      full.setRowKind(kind);
      rows.add(full);
      GenericRowData holes = GenericRowData.of(8L, null, null);
      holes.setRowKind(kind);
      rows.add(holes);
    }

    EncodeFormat format =
        EncodeFormat.of(
            "debezium-avro-confluent",
            Map.of("url", url, "schema-registry.subject", "t-value"),
            ROW_TYPE);
    assertNotNull(format);
    String options = format.openOptions();

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, ROW_TYPE, allocator, true);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][][] records =
          NativeKafka.encodeKafkaRecords(
              array.memoryAddress(),
              schema.memoryAddress(),
              format.format,
              options,
              format.format,
              options,
              LogicalTypeDescriptors.of(ROW_TYPE),
              ROW_TYPE.getFieldNames().toArray(String[]::new),
              new int[0],
              IntStream.range(0, ROW_TYPE.getFieldCount()).toArray(),
              false);

      assertEquals(rows.size(), records[1].length);
      for (int i = 0; i < rows.size(); i++) {
        byte[] expected = flink.serialize(rows.get(i));
        assertArrayEquals(
            expected,
            records[1][i],
            "row " + i + " (" + rows.get(i).getRowKind() + "): native "
                + new String(records[1][i], StandardCharsets.ISO_8859_1));
      }
    }
  }

  /**
   * The plan often feeds the sink generated expression names ({@code EXPR$0}, ...). The
   * envelope's image structs resolve their fields by the declared sink names, so the encode must
   * rename the batch first instead of failing schema resolution on a generated name.
   */
  @Test
  void matchesFlinkWhenThePlanRenamesColumns() throws Exception {
    String url = stubRegistry();
    DebeziumAvroSerializationSchema flink =
        new DebeziumAvroSerializationSchema(ROW_TYPE, url, "t-value", null);
    flink.open(null);

    RowType generated =
        RowType.of(
            ROW_TYPE.getChildren().toArray(LogicalType[]::new),
            new String[] {"EXPR$0", "EXPR$1", "EXPR$2"});
    GenericRowData insert = GenericRowData.of(7L, StringData.fromString("renamed"), 2.5);
    GenericRowData delete = GenericRowData.of(8L, null, null);
    delete.setRowKind(RowKind.DELETE);
    List<RowData> rows = List.of(insert, delete);

    EncodeFormat format =
        EncodeFormat.of(
            "debezium-avro-confluent",
            Map.of("url", url, "schema-registry.subject", "t-value"),
            ROW_TYPE);
    assertNotNull(format);
    String options = format.openOptions();

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, generated, allocator, true);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][][] records =
          NativeKafka.encodeKafkaRecords(
              array.memoryAddress(),
              schema.memoryAddress(),
              format.format,
              options,
              format.format,
              options,
              LogicalTypeDescriptors.of(ROW_TYPE),
              ROW_TYPE.getFieldNames().toArray(String[]::new),
              new int[0],
              IntStream.range(0, ROW_TYPE.getFieldCount()).toArray(),
              false);

      assertEquals(rows.size(), records[1].length);
      for (int i = 0; i < rows.size(); i++) {
        assertArrayEquals(flink.serialize(rows.get(i)), records[1][i], "row " + i);
      }
    }
  }

  @Test
  void gatesShapesFlinkRejectsOrTheEnvelopeCannotCarry() throws Exception {
    String url = stubRegistry();
    // The subject is required for serialization (Flink raises its own ValidationException).
    assertNull(EncodeFormat.of("debezium-avro-confluent", Map.of("url", url), ROW_TYPE));
    // Registry auth/ssl/properties options stay on Flink, like the decode side.
    assertNull(
        EncodeFormat.of(
            "debezium-avro-confluent",
            Map.of("url", url, "schema-registry.subject", "s", "basic-auth.user-info", "u:p"),
            ROW_TYPE));
    // The hard-wired legacy mapping cannot carry TIMESTAMP_LTZ — Flink's own derivation throws.
    RowType ltz =
        RowType.of(
            new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
            new String[] {"id", "ts"});
    assertNull(
        EncodeFormat.of(
            "debezium-avro-confluent",
            Map.of("url", url, "schema-registry.subject", "s"),
            ltz));
  }

  /** One stub registry serving Flink's Confluent client and the native POST alike: identical
   * schema strings get one id, the way a real registry deduplicates registrations. */
  private String stubRegistry() throws Exception {
    registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    Map<String, Integer> ids = new HashMap<>();
    registry.createContext(
        "/subjects",
        exchange -> {
          String posted =
              new ObjectMapper().readTree(exchange.getRequestBody()).get("schema").asText();
          int id;
          synchronized (ids) {
            id = ids.computeIfAbsent(posted, key -> 40 + ids.size());
          }
          byte[] body = ("{\"id\":" + id + "}").getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    registry.start();
    return "http://localhost:" + registry.getAddress().getPort();
  }
}
