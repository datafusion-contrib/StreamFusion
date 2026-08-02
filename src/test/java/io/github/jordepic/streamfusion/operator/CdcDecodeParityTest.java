package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.format.FormatCodes;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.json.CanalJsonFormatProvider;
import io.github.jordepic.streamfusion.format.json.DebeziumJsonFormatProvider;
import io.github.jordepic.streamfusion.format.json.MaxwellJsonFormatProvider;
import io.github.jordepic.streamfusion.format.json.OggJsonFormatProvider;
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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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

  @Test
  void debeziumNestedRowMatchesFlink() throws Exception {
    // Debezium/OGG route for nested schemas (unlike Maxwell/Canal, gated flat): the envelope's
    // images decode through the same nested appenders as plain JSON, RowKinds included.
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
    String envelope = "{\"before\":null,\"after\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"op\":\"c\"}";
    String second = "{\"before\":null,\"after\":{\"id\":2,\"name\":\"b\",\"score\":2.5},\"op\":\"c\"}";
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
    String maxwellWrapped = "[{\"data\":{\"id\":1,\"name\":\"a\",\"score\":1.5},\"type\":\"insert\"}]";
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
    return switch (format) {
      case MAXWELL -> new MaxwellJsonFormatProvider();
      case CANAL -> new CanalJsonFormatProvider();
      case DEBEZIUM -> new DebeziumJsonFormatProvider();
      case OGG -> new OggJsonFormatProvider();
      default -> throw new IllegalArgumentException("Unknown CDC format: " + format);
    };
  }
}
