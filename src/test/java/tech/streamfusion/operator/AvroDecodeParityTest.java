package tech.streamfusion.operator;

import tech.streamfusion.format.avro.AvroFormatProvider;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Pins the native bare-Avro decode to Flink's own {@link AvroRowDataDeserializationSchema},
 * message by message, across the full reconciled type family: both engines decode datums written
 * with the schema Flink derives from the row type, and the outcomes must match — identical rows
 * field for field, or both failing. Covers the conversions Flink applies on top of Avro's logical
 * types (wrapping TINYINT/SMALLINT narrowing, precision-overflow decimals decoding to NULL,
 * epoch-millis timestamps incl. pre-epoch values) and the nested container shapes whose Arrow
 * child layout the reconciliation rebuilds (ROW/ARRAY/MAP/MULTISET).
 */
@Tag("streamfusion-avro")
class AvroDecodeParityTest {

  // Non-nullable at the row level, as the planner's physical row type is: the deserializer derives
  // its reader schema from this type's own nullability (a nullable row would become a top-level
  // union the writer bytes don't carry).
  private static final RowType FULL_TYPE =
      (RowType)
          RowType.of(
                  new LogicalType[] {
            new BooleanType(),
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new VarBinaryType(VarBinaryType.MAX_LENGTH),
            new DecimalType(5, 2),
            new DateType(),
            new TimeType(3),
            new TimestampType(3),
            RowType.of(
                new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                new String[] {"x", "y"}),
            new ArrayType(new IntType()),
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
            new MultisetType(new VarCharType(VarCharType.MAX_LENGTH))
          },
          new String[] {
            "b", "ti", "si", "i", "l", "f", "d", "s", "vb", "dec", "dt", "t", "ts", "r", "arr",
            "m", "ms"
          })
              .copy(false);

  @Test
  void populatedRowMatchesFlinkFieldForField() throws Exception {
    assertParity(
        "populated row",
        record -> {
          record.put("b", true);
          record.put("ti", 1);
          record.put("si", 2);
          record.put("i", 3);
          record.put("l", 4L);
          record.put("f", 1.5f);
          record.put("d", 2.5);
          record.put("s", "hello");
          record.put("vb", ByteBuffer.wrap(new byte[] {1, -2, 3}));
          record.put("dec", ByteBuffer.wrap(new byte[] {0x30, 0x39})); // unscaled 12345 -> 123.45
          record.put("dt", 19_000);
          record.put("t", 45_296_789);
          record.put("ts", 1_577_934_245_678L);
          GenericRecord nested = new GenericData.Record(nestedSchema());
          nested.put("x", 7L);
          nested.put("y", "n");
          record.put("r", nested);
          record.put("arr", List.of(1, 2, 3));
          Map<String, Long> m = new LinkedHashMap<>();
          m.put("k1", 10L);
          m.put("k2", 20L);
          record.put("m", m);
          record.put("ms", Map.of("tag", 2));
        });
  }

  @Test
  void flinkQuirksAreReproduced() throws Exception {
    // Out-of-range avro ints narrow into TINYINT/SMALLINT with Java's wrapping byteValue()/
    // shortValue(); a decimal whose digits exceed the declared precision decodes to NULL.
    assertParity(
        "wrapping small ints and overflowing decimal",
        record -> {
          record.put("ti", 300);
          record.put("si", 70_000);
          record.put("dec", ByteBuffer.wrap(new byte[] {0x01, (byte) 0xE2, 0x40})); // 123456
        });
    assertParity(
        "pre-epoch temporal values",
        record -> {
          record.put("ts", -1L);
          record.put("dt", -10);
          record.put("t", 0);
        });
  }

  @Test
  void nullsAndEmptyContainersMatch() throws Exception {
    assertParity("all null", record -> {});
    assertParity(
        "empty containers",
        record -> {
          record.put("arr", List.of());
          record.put("m", Map.of());
        });
  }

  @Test
  void bothEnginesRejectAMalformedDatum() throws Exception {
    assertParity("malformed datum", new byte[] {0x02});
    assertParity("truncated datum", truncated());
  }

  @Test
  void bytesAfterTheFirstDatumAreIgnoredLikeFlink() throws Exception {
    // Flink reads exactly one datum per message and never checks the remaining buffer: trailing
    // junk after a complete datum is ignored, and a second concatenated datum is dead bytes — one
    // row either way, from the first datum.
    byte[] first =
        encode(
            record -> {
              record.put("l", 7L);
              record.put("s", "first");
            });
    byte[] second =
        encode(
            record -> {
              record.put("l", 8L);
              record.put("s", "second");
            });
    assertParity("datum with trailing junk", concat(first, new byte[] {(byte) 0xFF, 0x07, 0x00}));
    assertParity("two concatenated datums", concat(first, second));
  }

  private static byte[] concat(byte[] head, byte[] tail) {
    byte[] whole = Arrays.copyOf(head, head.length + tail.length);
    System.arraycopy(tail, 0, whole, head.length, tail.length);
    return whole;
  }

  @Test
  void correctedTimestampMappingMatchesFlink() throws Exception {
    // avro.timestamp_mapping.legacy = false: TIMESTAMP derives local-timestamp-millis/micros and
    // TIMESTAMP_LTZ becomes representable as timestamp-millis/micros. Flink still reads every
    // long as epoch millis (its converter has no micros path) — the values below make the
    // micros-declared columns visibly off by 1000x under a spec-faithful read, so parity here
    // pins the reproduced quirk end to end.
    RowType corrected =
        (RowType)
            RowType.of(
                    new LogicalType[] {
                      new TimestampType(3),
                      new TimestampType(6),
                      new LocalZonedTimestampType(3),
                      new LocalZonedTimestampType(6)
                    },
                    new String[] {"ts3", "ts6", "ltz3", "ltz6"})
                .copy(false);
    Schema writer = AvroSchemaConverter.convertToSchema(corrected, false);
    Map<String, String> options =
        Map.of("format", "avro", "avro.timestamp_mapping.legacy", "false");
    Consumer<GenericRecord> filler =
        record -> {
          record.put("ts3", 1_577_934_245_678L);
          record.put("ts6", 1_577_934_245_678L);
          record.put("ltz3", -1L);
          record.put("ltz6", 1_577_934_245_678L);
        };
    assertParity("corrected mapping", corrected, writer, filler, options, false);
    assertParity("corrected mapping, all null", corrected, writer, record -> {}, options, false);
  }

  private static byte[] truncated() throws Exception {
    byte[] whole =
        encode(
            record -> {
              record.put("s", "a string long enough to survive truncation");
              record.put("l", 42L);
            });
    return Arrays.copyOf(whole, whole.length - 5);
  }

  private static Schema writerSchema() {
    return AvroSchemaConverter.convertToSchema(FULL_TYPE);
  }

  private static Schema nestedSchema() {
    Schema field = writerSchema().getField("r").schema();
    return field.getTypes().stream()
        .filter(s -> s.getType() == Schema.Type.RECORD)
        .findFirst()
        .orElseThrow();
  }

  private static byte[] encode(Schema schema, Consumer<GenericRecord> filler) throws Exception {
    GenericRecord record = new GenericData.Record(schema);
    filler.accept(record);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static byte[] encode(Consumer<GenericRecord> filler) throws Exception {
    return encode(writerSchema(), filler);
  }

  private static void assertParity(String label, Consumer<GenericRecord> filler) throws Exception {
    assertParity(label, encode(filler));
  }

  private static void assertParity(String label, byte[] message) {
    assertParity(label, FULL_TYPE, message, Map.of("format", "avro"), true);
  }

  private static void assertParity(
      String label,
      RowType rowType,
      Schema writer,
      Consumer<GenericRecord> filler,
      Map<String, String> options,
      boolean legacyTimestampMapping)
      throws Exception {
    assertParity(label, rowType, encode(writer, filler), options, legacyTimestampMapping);
  }

  private static void assertParity(
      String label,
      RowType rowType,
      byte[] message,
      Map<String, String> options,
      boolean legacyTimestampMapping) {
    DecodeParityHarness harness = new DecodeParityHarness(rowType, false);
    harness.assertParity(
        label,
        () -> flinkDecode(harness, rowType, message, legacyTimestampMapping),
        () -> harness.nativeDecode(new AvroFormatProvider(), message, options, false));
  }

  private static List<List<Object>> flinkDecode(
      DecodeParityHarness harness, RowType rowType, byte[] message, boolean legacyTimestampMapping)
      throws Exception {
    AvroRowDataDeserializationSchema schema =
        new AvroRowDataDeserializationSchema(
            rowType, InternalTypeInfo.of(rowType), AvroEncoding.BINARY, legacyTimestampMapping);
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(AvroDecodeParityTest.class.getClassLoader());
          }
        });
    RowData row = schema.deserialize(message);
    return row == null ? List.of() : List.of(harness.fields(row));
  }
}
