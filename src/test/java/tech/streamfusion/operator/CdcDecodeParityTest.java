package tech.streamfusion.operator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.formats.json.maxwell.MaxwellJsonDeserializationSchema;
import org.apache.flink.formats.json.ogg.OggJsonDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.json.CanalJsonFormatProvider;
import tech.streamfusion.format.json.DebeziumJsonFormatProvider;
import tech.streamfusion.format.json.MaxwellJsonFormatProvider;
import tech.streamfusion.format.json.OggJsonFormatProvider;

/**
 * Pins the native CDC decode to Flink's own Maxwell/Canal/Debezium/OGG deserializers, message by
 * message (no containers — the format classes referee directly, like {@link CsvDecodeParityTest}).
 * The heart of it is the partial-{@code old} pre-image rule that used to gate Maxwell/Canal off the
 * native path: Flink reads an UPDATE_BEFORE field from {@code old} when its KEY is present
 * anywhere under it ({@code findValue}'s recursive search) — an explicit null means "was null" and
 * stays null; an absent key means "unchanged" and copies the post-image — and for Canal the
 * presence check spans the WHOLE {@code old} array ({@code findValue} over the array node). The
 * native decode reproduces that with a per-message key scan of the raw {@code old} JSON; every
 * scenario here must produce the same changelog (RowKinds included) from both engines, or fail on
 * both.
 */
@Tag("streamfusion-json")
class CdcDecodeParityTest {
  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
          new String[] {"id", "name", "score"});

  private static final DecodeParityHarness HARNESS = new DecodeParityHarness(ROW_TYPE, true);

  private static final RowType NESTED_ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            RowType.of(
                new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
                new String[] {"name", "score"}),
            new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
            new MultisetType(new VarCharType(VarCharType.MAX_LENGTH)),
            new ArrayType(
                RowType.of(
                    new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                    new String[] {"id", "name"}))
          },
          new String[] {"id", "name", "info", "tags", "attrs", "bag", "items"});

  private static final int MAXWELL = FormatCodes.MAXWELL_JSON;
  private static final int CANAL = FormatCodes.CANAL_JSON;
  private static final int DEBEZIUM = FormatCodes.DEBEZIUM_JSON;
  private static final int OGG = FormatCodes.OGG_JSON;

  @Test
  void maxwellMatchesFlinkPerMessage() throws Exception {
    String[] scenarios = {
      "{\"data\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"type\":\"insert\"}",
      // Partial old: absent fields copy from data; present fields keep the old value.
      "{\"data\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"old\":{\"name\":\"a\"},\"type\":\"update\"}",
      // A field changed TO a value FROM null: old carries an explicit null — kept, not copied.
      "{\"data\":{\"id\":1,\"name\":\"was-null\",\"score\":1.5},\"old\":{\"name\":null},\"type\":\"update\"}",
      "{\"data\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"type\":\"delete\"}",
      // Corrupt shapes: both engines must fail (or both skip under ignore-parse-errors).
      "{\"data\":{\"id\":1,\"name\":\"x\",\"score\":1.5},\"type\":\"update\"}",
      "{\"data\":{\"id\":1,\"name\":\"x\",\"score\":1.5},\"type\":\"upsert\"}",
      "{\"data\":null,\"type\":\"insert\"}",
    };
    for (String scenario : scenarios) {
      assertParity(MAXWELL, scenario, false);
      assertParity(MAXWELL, scenario, true);
    }
  }

  @Test
  void whitespaceBodiesAreNotTombstones() throws Exception {
    // Flink's tombstone check is message.length == 0: an empty message is skipped, but a
    // whitespace-only body reaches Jackson, yields no envelope, and corrupts — a job failure in
    // strict mode, a whole-message drop under ignore-parse-errors. Per dialect, per mode.
    for (int format : new int[] {MAXWELL, CANAL, DEBEZIUM, OGG}) {
      for (String scenario : new String[] {"", "   ", " \n\t "}) {
        assertParity(format, scenario, false);
        assertParity(format, scenario, true);
      }
    }
  }

  @Test
  void maxwellOldPresenceIsRecursiveLikeFindValue() throws Exception {
    String[] scenarios = {
      // findValue descends nested containers: a field name buried under old counts as PRESENT, so
      // UPDATE_BEFORE keeps the top-level null instead of copying the post-image value.
      "{\"data\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"old\":{\"junk\":{\"score\":9}},\"type\":\"update\"}",
      "{\"data\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"old\":{\"junk\":[{\"name\":\"x\"}]},\"type\":\"update\"}",
      // Jackson's tree collapses a duplicate key to its LAST occurrence — for the envelope's old
      // field and for objects inside it — so names reachable only through a discarded earlier
      // subtree are not found.
      "{\"data\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"old\":{\"name\":\"x\"},\"old\":{\"score\":9},\"type\":\"update\"}",
      "{\"data\":{\"id\":1,\"name\":\"a2\",\"score\":1.5},\"old\":{\"junk\":{\"score\":9},\"junk\":{\"keep\":1}},\"type\":\"update\"}",
    };
    for (String scenario : scenarios) {
      assertParity(MAXWELL, scenario, false);
      assertParity(MAXWELL, scenario, true);
    }
  }

  /**
   * The CDC dialects decode through Flink's TREE deserializer, so duplicate keys inside an image
   * collapse last-wins with no field-counter saturation — unlike plain json's parser path, where
   * the counter would exhaust the arity on the duplicate pair and skip the remaining fields.
   */
  @Test
  void maxwellImageDuplicateKeysCollapseLastWinsWithoutSaturation() throws Exception {
    String scenario =
        "{\"data\":{\"id\":1,\"id\":2,\"name\":\"a\",\"score\":1.5},\"type\":\"insert\"}";
    assertParity(MAXWELL, scenario, false);
    assertParity(MAXWELL, scenario, true);
  }

  @Test
  void canalOldPresenceIsRecursiveLikeFindValue() throws Exception {
    String[] scenarios = {
      "{\"data\":[{\"id\":1,\"name\":\"a2\",\"score\":1.5}],\"old\":[{\"junk\":{\"score\":9}}],\"type\":\"UPDATE\"}",
      "{\"data\":[{\"id\":1,\"name\":\"a2\",\"score\":1.5}],\"old\":[{\"junk\":[{\"name\":\"x\"}]}],\"type\":\"UPDATE\"}",
      "{\"data\":[{\"id\":1,\"name\":\"a2\",\"score\":1.5}],\"old\":[{\"name\":\"a\"}],\"old\":[{\"score\":9}],\"type\":\"UPDATE\"}",
    };
    for (String scenario : scenarios) {
      assertParity(CANAL, scenario, false);
      assertParity(CANAL, scenario, true);
    }
  }

  @Test
  void canalMatchesFlinkPerMessage() throws Exception {
    String[] scenarios = {
      // Multi-row fan-out.
      "{\"data\":[{\"id\":1,\"name\":\"a\",\"score\":1.5},{\"id\":2,\"name\":\"b\",\"score\":2.5}],"
          + "\"type\":\"INSERT\"}",
      // Paired update arrays with partial old.
      "{\"data\":[{\"id\":1,\"name\":\"a2\",\"score\":1.5}],\"old\":[{\"name\":\"a\"}],\"type\":\"UPDATE\"}",
      // The findValue quirk: presence is message-wide across old's elements, so element 0's null id
      // is KEPT (id appears in old[1]) rather than copied from data.
      "{\"data\":[{\"id\":1,\"name\":\"a2\",\"score\":1.5},{\"id\":2,\"name\":\"b2\",\"score\":2.5}],"
          + "\"old\":[{\"name\":\"a\"},{\"id\":2}],\"type\":\"UPDATE\"}",
      "{\"data\":[{\"id\":1,\"name\":\"a\",\"score\":1.5}],\"type\":\"DELETE\"}",
      // DDL is skipped by both; corrupt shapes fail on both.
      "{\"data\":null,\"type\":\"CREATE\"}",
      "{\"data\":[{\"id\":1,\"name\":\"x\",\"score\":1.5},{\"id\":2,\"name\":\"y\",\"score\":2.5}],"
          + "\"old\":[{\"name\":\"w\"}],\"type\":\"UPDATE\"}",
      "{\"data\":null,\"type\":\"INSERT\"}",
      "{\"data\":[{\"id\":1,\"name\":\"x\",\"score\":1.5}],\"type\":\"TRUNCATE\"}",
    };
    for (String scenario : scenarios) {
      assertParity(CANAL, scenario, false);
      assertParity(CANAL, scenario, true);
    }
  }

  /**
   * Maxwell/Canal with ROW, ARRAY, MAP and MULTISET columns. {@code findValue} searches the whole
   * {@code old} subtree, so a top-level column whose name also appears as a nested key — a ROW
   * field, an element row's field, a MAP key or a MULTISET element — counts as present and keeps
   * the UPDATE_BEFORE null instead of copying the post-image.
   */
  @Test
  void maxwellNestedColumnsMatchFlink() throws Exception {
    DecodeParityHarness harness = new DecodeParityHarness(NESTED_ROW_TYPE, true);
    String data =
        "{\"id\":1,\"name\":\"a2\",\"info\":{\"name\":\"n2\",\"score\":2.5},"
            + "\"tags\":[\"t2\"],\"attrs\":{\"k\":2},\"bag\":{\"b\":2},"
            + "\"items\":[{\"id\":9,\"name\":\"i2\"}]}";
    String[] scenarios = {
      "{\"data\":" + data + ",\"type\":\"insert\"}",
      "{\"data\":" + data + ",\"type\":\"delete\"}",
      // Only a nested ROW field changed: `name` is found inside old.info, so the top-level name
      // stays NULL in UPDATE_BEFORE while id and the other columns copy from data.
      "{\"data\":" + data + ",\"old\":{\"info\":{\"name\":\"n1\",\"score\":1.5}},\"type\":\"update\"}",
      // A partial nested ROW is the pre-image ROW as written — sub-fields are not merged.
      "{\"data\":" + data + ",\"old\":{\"info\":{\"score\":1.5}},\"type\":\"update\"}",
      "{\"data\":" + data + ",\"old\":{\"name\":\"a1\"},\"type\":\"update\"}",
      // MAP keys and MULTISET elements are object keys, so they count as present names.
      "{\"data\":" + data + ",\"old\":{\"attrs\":{\"id\":7}},\"type\":\"update\"}",
      "{\"data\":" + data + ",\"old\":{\"bag\":{\"name\":3}},\"type\":\"update\"}",
      // An element row's fields inside an ARRAY are searched too.
      "{\"data\":" + data + ",\"old\":{\"items\":[{\"id\":8,\"name\":\"i1\"}]},\"type\":\"update\"}",
      // Explicit nested nulls are present and kept.
      "{\"data\":" + data + ",\"old\":{\"info\":null,\"tags\":null,\"items\":null},\"type\":\"update\"}",
      // A duplicate key collapses to its last value, discarding names only the first one held.
      "{\"data\":" + data + ",\"old\":{\"info\":{\"name\":\"n1\"},\"info\":{\"score\":1.5}},"
          + "\"type\":\"update\"}",
      "{\"data\":" + data + ",\"old\":{\"tags\":[\"t1\",null],\"attrs\":{\"k\":1,\"j\":null}},"
          + "\"type\":\"update\"}",
      // A nested value of the wrong shape is a per-field conversion failure.
      "{\"data\":" + data + ",\"old\":{\"info\":\"oops\"},\"type\":\"update\"}",
      "{\"data\":{\"id\":1,\"info\":[1,2]},\"type\":\"insert\"}",
    };
    for (String scenario : scenarios) {
      assertParity(harness, NESTED_ROW_TYPE, MAXWELL, scenario, false);
      assertParity(harness, NESTED_ROW_TYPE, MAXWELL, scenario, true);
    }
  }

  @Test
  void canalNestedColumnsMatchFlink() throws Exception {
    DecodeParityHarness harness = new DecodeParityHarness(NESTED_ROW_TYPE, true);
    String first =
        "{\"id\":1,\"name\":\"a2\",\"info\":{\"name\":\"n2\",\"score\":2.5},"
            + "\"tags\":[\"t2\"],\"attrs\":{\"k\":2},\"bag\":{\"b\":2},\"items\":[]}";
    String second =
        "{\"id\":2,\"name\":\"b2\",\"info\":null,\"tags\":[],\"attrs\":{},\"bag\":null,"
            + "\"items\":[{\"id\":9,\"name\":\"i2\"}]}";
    String data = "[" + first + "," + second + "]";
    String[] scenarios = {
      "{\"data\":" + data + ",\"type\":\"INSERT\"}",
      "{\"data\":" + data + ",\"type\":\"DELETE\"}",
      // Presence spans the whole old array: element 1's nested `name` keeps element 0's top-level
      // name NULL too.
      "{\"data\":" + data + ",\"old\":[{\"tags\":[\"t1\"]},{\"items\":[{\"name\":\"i1\"}]}],"
          + "\"type\":\"UPDATE\"}",
      "{\"data\":" + data + ",\"old\":[{\"info\":{\"score\":1.5}},{\"attrs\":{\"id\":1}}],"
          + "\"type\":\"UPDATE\"}",
      "{\"data\":" + data + ",\"old\":[{\"bag\":{\"info\":1}},{\"name\":null}],\"type\":\"UPDATE\"}",
      "{\"data\":" + data + ",\"old\":[{\"info\":{\"name\":\"x\"},\"info\":{\"score\":1}},{}],"
          + "\"type\":\"UPDATE\"}",
      "{\"data\":" + data + ",\"old\":[{\"info\":\"oops\"},{}],\"type\":\"UPDATE\"}",
    };
    for (String scenario : scenarios) {
      assertParity(harness, NESTED_ROW_TYPE, CANAL, scenario, false);
      assertParity(harness, NESTED_ROW_TYPE, CANAL, scenario, true);
    }
  }

  /**
   * The CDC dialects convert through Flink's tree deserializer, whose MAP and MULTISET values also
   * fill a {@code java.util.HashMap}: entries iterate in its bucket order through resizes and tree
   * bins, here forced by fourteen keys sharing a bucket of a 64-slot table.
   */
  @Test
  void cdcMapEntriesIterateInFlinksHashMapOrder() throws Exception {
    DecodeParityHarness harness = new DecodeParityHarness(NESTED_ROW_TYPE, true);
    StringBuilder attrs = new StringBuilder("{");
    int collisions = 0;
    for (int i = 0; collisions < 14; i++) {
      int hash = StringData.fromString("t" + i).hashCode();
      if (((hash ^ (hash >>> 16)) & 63) == 5) {
        attrs.append(collisions == 0 ? "" : ",").append("\"t").append(i).append("\":").append(i);
        collisions++;
      }
    }
    for (int i = 0; i < 30; i++) {
      attrs.append(",\"k").append(i).append("\":").append(i);
    }
    attrs.append('}');
    String data = "{\"id\":1,\"attrs\":" + attrs + ",\"bag\":" + attrs + "}";
    String[] scenarios = {
      "{\"data\":" + data + ",\"type\":\"insert\"}",
      "{\"data\":" + data + ",\"old\":{\"attrs\":" + attrs + "},\"type\":\"update\"}",
    };
    for (String scenario : scenarios) {
      assertParity(harness, NESTED_ROW_TYPE, MAXWELL, scenario, false);
      assertParity(harness, NESTED_ROW_TYPE, MAXWELL, scenario, true);
    }
  }

  /** Presence is per column, so the schema width is not capped at the old 128-bit mask. */
  @Test
  void maxwellAndCanalSchemasWiderThan128Columns() throws Exception {
    int width = 200;
    LogicalType[] types = new LogicalType[width];
    String[] names = new String[width];
    StringBuilder data = new StringBuilder("{");
    for (int i = 0; i < width; i++) {
      types[i] = new BigIntType();
      names[i] = "c" + i;
      data.append(i == 0 ? "" : ",").append("\"c").append(i).append("\":").append(i);
    }
    data.append('}');
    RowType wide = RowType.of(types, names);
    DecodeParityHarness harness = new DecodeParityHarness(wide, true);
    String maxwellUpdate =
        "{\"data\":" + data + ",\"old\":{\"c0\":-1,\"c127\":null,\"c128\":-128,"
            + "\"nested\":{\"c199\":1}},\"type\":\"update\"}";
    String canalUpdate =
        "{\"data\":[" + data + "," + data + "],\"old\":[{\"c64\":-64},{\"c150\":null,"
            + "\"deep\":[{\"c190\":1}]}],\"type\":\"UPDATE\"}";
    for (boolean skipErrors : new boolean[] {false, true}) {
      assertParity(harness, wide, MAXWELL, maxwellUpdate, skipErrors);
      assertParity(harness, wide, MAXWELL, "{\"data\":" + data + ",\"type\":\"insert\"}", skipErrors);
      assertParity(harness, wide, CANAL, canalUpdate, skipErrors);
    }
  }

  @Test
  void debeziumNestedRowMatchesFlink() throws Exception {
    // Nested images decode through the same appenders as plain JSON, RowKinds included.
    RowType nested =
        RowType.of(
            new LogicalType[] {
              new BigIntType(),
              RowType.of(
                  new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
                  new String[] {"name", "score"})
            },
            new String[] {"id", "info"});
    DecodeParityHarness harness = new DecodeParityHarness(nested, true);
    String[] scenarios = {
      "{\"before\":null,\"after\":{\"id\":1,\"info\":{\"name\":\"a\",\"score\":1.5}},\"op\":\"c\"}",
      // A nested update with coercions inside the images; missing sub-fields are null.
      "{\"before\":{\"id\":1,\"info\":{\"name\":\"a\",\"score\":\"1.5\"}},"
          + "\"after\":{\"id\":1,\"info\":{\"score\":2.5}},\"op\":\"u\"}",
      "{\"before\":{\"id\":1,\"info\":null},\"after\":null,\"op\":\"d\"}",
      "{\"before\":null,\"after\":{\"id\":1,\"info\":{\"name\":\"a\",\"score\":true}},\"op\":\"c\"}",
    };
    for (String scenario : scenarios) {
      for (boolean skipErrors : new boolean[] {false, true}) {
        harness.assertParity(
            scenario,
            () -> {
              DebeziumJsonDeserializationSchema schema =
                  new DebeziumJsonDeserializationSchema(
                      TypeConversions.fromLogicalToDataType(nested),
                      List.of(),
                      InternalTypeInfo.of(nested),
                      false,
                      skipErrors,
                      TimestampFormat.SQL);
              schema.open(null);
              List<List<Object>> rows = new ArrayList<>();
              schema.deserialize(
                  scenario.getBytes(StandardCharsets.UTF_8),
                  new Collector<>() {
                    @Override
                    public void collect(RowData row) {
                      rows.add(harness.fields(row));
                    }

                    @Override
                    public void close() {}
                  });
              return rows;
            },
            () ->
                harness.nativeDecode(
                    new DebeziumJsonFormatProvider(),
                    scenario,
                    Map.of("format", "debezium-json"),
                    skipErrors));
      }
    }
  }

  @Test
  void topLevelArrayMessagesMatchFlink() throws Exception {
    // The plain json format fans a top-level array out into one row per element, but a CDC
    // envelope never fans out. Maxwell/Canal hand the root to the tree converter, so any array
    // root is corrupt. Debezium/OGG decode through Flink's deprecated one-row deserialize, which
    // unwraps an array holding exactly one envelope — and under ignore-parse-errors skips junk
    // elements inside the fan-out loop first, so [{envelope}, 1] still unwraps there while every
    // other shape stays corrupt. Both engines must agree scenario by scenario, mode by mode.
    String envelope =
        "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op\":\"c\"}";
    String second =
        "{\"before\":null,\"after\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"op\":\"c\"}";
    String oggEnvelope =
        "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op_type\":\"I\"}";
    String[] debeziumScenarios = {
      "[" + envelope + "]",
      " [ " + envelope + " ] ",
      "[" + envelope + "," + second + "]",
      "[" + envelope + ",1]",
      "[" + envelope + ",{}]",
      "[{}]",
      "[1]",
      "[]",
    };
    String maxwellWrapped =
        "[{\"data\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"type\":\"insert\"}]";
    String canalWrapped =
        "[{\"data\":[{\"id\":1,\"name\":\"a\",\"score\":1.5}],\"type\":\"INSERT\"}]";
    for (boolean skipErrors : new boolean[] {false, true}) {
      for (String scenario : debeziumScenarios) {
        assertParity(DEBEZIUM, scenario, skipErrors);
      }
      assertParity(OGG, "[" + oggEnvelope + "]", skipErrors);
      assertParity(OGG, "[" + oggEnvelope + ",1]", skipErrors);
      assertParity(MAXWELL, maxwellWrapped, skipErrors);
      assertParity(CANAL, canalWrapped, skipErrors);
    }
  }

  @Test
  void debeziumArrayUnwrapMatchesFlinkOnDecimalSchemas() throws Exception {
    // DECIMAL-bearing schemas decode via the raw-literals (arrow-json) path, which classifies
    // array roots before decoding — the unwrap matrix must match the simd path above.
    RowType decimal =
        RowType.of(
            new LogicalType[] {new BigIntType(), new DecimalType(10, 2)},
            new String[] {"id", "amount"});
    DecodeParityHarness harness = new DecodeParityHarness(decimal, true);
    String envelope =
        "{\"before\":null,\"after\":{\"id\":1,\"amount\":12.345},\"op\":\"c\"}";
    String[] scenarios = {
      "[" + envelope + "]",
      "[" + envelope + "," + envelope + "]",
      "[" + envelope + ",1]",
      "[]",
    };
    for (boolean skipErrors : new boolean[] {false, true}) {
      for (String scenario : scenarios) {
        assertParity(harness, decimal, DEBEZIUM, scenario, skipErrors);
      }
    }
  }

  @Test
  void debeziumNullImagesMatchFlink() throws Exception {
    String[] scenarios = {
      "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op\":\"c\"}",
      // Null images where the op reads them: Flink NPEs (corrupt message), the native decode fails.
      "{\"before\":null,\"after\":null,\"op\":\"c\"}",
      "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op\":\"u\"}",
      "{\"before\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"after\":null,\"op\":\"u\"}",
    };
    for (String scenario : scenarios) {
      assertParity(DEBEZIUM, scenario, false);
      assertParity(DEBEZIUM, scenario, true);
    }
  }

  private static void assertParity(int format, String message, boolean skipErrors) {
    assertParity(HARNESS, ROW_TYPE, format, message, skipErrors);
  }

  private static void assertParity(
      DecodeParityHarness harness, RowType rowType, int format, String message, boolean skipErrors) {
    harness.assertParity(
        message,
        () -> flinkDecode(harness, rowType, format, message, skipErrors),
        () ->
            harness.nativeDecode(
                provider(format),
                message,
                Map.of("format", provider(format).formatIdentifier()),
                skipErrors));
  }

  private static List<List<Object>> flinkDecode(
      DecodeParityHarness harness, RowType rowType, int format, String message, boolean ignoreErrors)
      throws Exception {
    DataType physical = TypeConversions.fromLogicalToDataType(rowType);
    InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowType);
    DeserializationSchema<RowData> schema;
    switch (format) {
      case MAXWELL:
        schema =
            new MaxwellJsonDeserializationSchema(
                physical, List.of(), typeInfo, ignoreErrors, TimestampFormat.SQL);
        break;
      case CANAL:
        schema =
            CanalJsonDeserializationSchema.builder(physical, List.of(), typeInfo)
                .setIgnoreParseErrors(ignoreErrors)
                .build();
        break;
      case OGG:
        schema =
            new OggJsonDeserializationSchema(
                physical, List.of(), typeInfo, ignoreErrors, TimestampFormat.SQL);
        break;
      default:
        schema =
            new DebeziumJsonDeserializationSchema(
                physical, List.of(), typeInfo, false, ignoreErrors, TimestampFormat.SQL);
    }
    schema.open(null);
    List<List<Object>> rows = new ArrayList<>();
    schema.deserialize(
        message.getBytes(StandardCharsets.UTF_8),
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

  private static NativeFormatProvider provider(int format) {
    switch (format) {
      case MAXWELL:
        return new MaxwellJsonFormatProvider();
      case CANAL:
        return new CanalJsonFormatProvider();
      case DEBEZIUM:
        return new DebeziumJsonFormatProvider();
      case OGG:
        return new OggJsonFormatProvider();
      default:
        throw new IllegalArgumentException("Unknown CDC format: " + format);
    }
  }
}
