package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.LogicalTypeDescriptors;
import tech.streamfusion.operator.RowDataArrowConverter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.canal.CanalJsonSerializationSchema;
import org.apache.flink.formats.json.debezium.DebeziumJsonSerializationSchema;
import org.apache.flink.formats.json.maxwell.MaxwellJsonSerializationSchema;
import org.apache.flink.formats.json.ogg.OggJsonSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Byte-level referee for the four CDC JSON envelope encoders against Flink's own serialization
 * schemas: every dialect, every row kind, null-field rows, both {@code encode.ignore-null-fields}
 * modes (which also drop the envelope's null {@code before}/{@code after} key, as Flink's shared
 * envelope serializer does), and both timestamp standards.
 */
@Tag("streamfusion-kafka")
class NativeKafkaCdcEncoderTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new IntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new TimestampType(3),
            new LocalZonedTimestampType(3),
            new DecimalType(10, 2)
          },
          new String[] {"id", "name", "ts", "instant", "amount"});

  @Test
  void debeziumMatchesFlinkForAllRowKindsAndOptionModes() throws Exception {
    assertMatchesFlink("debezium-json", NativeKafkaCdcEncoderTest::debezium);
  }

  @Test
  void canalMatchesFlinkForAllRowKindsAndOptionModes() throws Exception {
    assertMatchesFlink("canal-json", NativeKafkaCdcEncoderTest::canal);
  }

  @Test
  void maxwellMatchesFlinkForAllRowKindsAndOptionModes() throws Exception {
    assertMatchesFlink("maxwell-json", NativeKafkaCdcEncoderTest::maxwell);
  }

  @Test
  void oggMatchesFlinkForAllRowKindsAndOptionModes() throws Exception {
    assertMatchesFlink("ogg-json", NativeKafkaCdcEncoderTest::ogg);
  }

  /** schema-include is rejected by Flink's debezium sink factory, so it must not resolve natively. */
  @Test
  void debeziumSchemaIncludeStaysOnFlink() {
    assertNull(EncodeFormat.of("debezium-json", Map.of("schema-include", "true"), ROW_TYPE));
    assertNotNull(EncodeFormat.of("debezium-json", Map.of("schema-include", "false"), ROW_TYPE));
  }

  /** Canal's database/table filters are deserialization-only; Flink ignores them on write. */
  @Test
  void canalIgnoresDeserializationOnlyFilters() {
    assertNotNull(
        EncodeFormat.of(
            "canal-json",
            Map.of("database.include", "mydb", "table.include", "orders"),
            ROW_TYPE));
  }

  private interface FlinkCdcSchema {
    SerializationSchema<RowData> create(
        RowType rowType,
        TimestampFormat timestampFormat,
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
        String mapNullKeyLiteral,
        boolean decimalAsPlainNumber,
        boolean ignoreNullFields);
  }

  private static SerializationSchema<RowData> debezium(
      RowType rowType,
      TimestampFormat timestampFormat,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    return new DebeziumJsonSerializationSchema(
        rowType, timestampFormat, mode, literal, plainDecimal, ignoreNulls);
  }

  private static SerializationSchema<RowData> canal(
      RowType rowType,
      TimestampFormat timestampFormat,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    return new CanalJsonSerializationSchema(
        rowType, timestampFormat, mode, literal, plainDecimal, ignoreNulls);
  }

  private static SerializationSchema<RowData> maxwell(
      RowType rowType,
      TimestampFormat timestampFormat,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    return new MaxwellJsonSerializationSchema(
        rowType, timestampFormat, mode, literal, plainDecimal, ignoreNulls);
  }

  private static SerializationSchema<RowData> ogg(
      RowType rowType,
      TimestampFormat timestampFormat,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    return new OggJsonSerializationSchema(
        rowType, timestampFormat, mode, literal, plainDecimal, ignoreNulls);
  }

  /** One full and one null-holed row under each of the four row kinds. */
  private static List<RowData> changelogRows() {
    TimestampData ts = TimestampData.fromEpochMillis(1_577_934_245_678L);
    List<RowData> rows = new ArrayList<>();
    for (RowKind kind : RowKind.values()) {
      GenericRowData full =
          GenericRowData.of(
              1,
              StringData.fromString("quote: \" and 雪"),
              ts,
              ts,
              DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2));
      full.setRowKind(kind);
      rows.add(full);
      GenericRowData holes = GenericRowData.of(2, null, null, ts, null);
      holes.setRowKind(kind);
      rows.add(holes);
    }
    return rows;
  }

  private static void assertMatchesFlink(String identifier, FlinkCdcSchema flinkSchema)
      throws Exception {
    for (TimestampFormat timestampFormat :
        new TimestampFormat[] {TimestampFormat.SQL, TimestampFormat.ISO_8601}) {
      for (boolean ignoreNullFields : new boolean[] {false, true}) {
        assertMatchesFlink(identifier, flinkSchema, timestampFormat, ignoreNullFields);
      }
    }
  }

  private static void assertMatchesFlink(
      String identifier,
      FlinkCdcSchema flinkSchema,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields)
      throws Exception {
    List<RowData> rows = changelogRows();
    SerializationSchema<RowData> flink =
        flinkSchema.create(
            ROW_TYPE,
            timestampFormat,
            JsonFormatOptions.MapNullKeyMode.FAIL,
            "null",
            false,
            ignoreNullFields);
    flink.open(initializationContext());

    EncodeFormat format =
        EncodeFormat.of(
            identifier,
            Map.of(
                "timestamp-format.standard",
                timestampFormat == TimestampFormat.SQL ? "SQL" : "ISO-8601",
                "encode.ignore-null-fields",
                String.valueOf(ignoreNullFields)),
            ROW_TYPE);
    assertNotNull(format, identifier);

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
              format.options,
              format.format,
              format.options,
              LogicalTypeDescriptors.of(ROW_TYPE),
              ROW_TYPE.getFieldNames().toArray(String[]::new),
              new int[0],
              IntStream.range(0, ROW_TYPE.getFieldCount()).toArray(),
              false);

      assertEquals(rows.size(), records[1].length);
      for (int i = 0; i < rows.size(); i++) {
        assertNull(records[0][i], "CDC records on an ordinary kafka table carry no key");
        byte[] expected = flink.serialize(rows.get(i));
        assertArrayEquals(
            expected,
            records[1][i],
            identifier
                + " row "
                + i
                + " ("
                + rows.get(i).getRowKind()
                + "): expected "
                + new String(expected, StandardCharsets.UTF_8)
                + ", actual "
                + new String(records[1][i], StandardCharsets.UTF_8));
      }
    }
  }

  private static SerializationSchema.InitializationContext initializationContext() {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(NativeKafkaCdcEncoderTest.class.getClassLoader());
      }
    };
  }
}
