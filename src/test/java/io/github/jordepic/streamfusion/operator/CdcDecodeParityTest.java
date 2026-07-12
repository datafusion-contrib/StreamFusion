package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.json.CanalJsonFormatProvider;
import io.github.jordepic.streamfusion.format.json.DebeziumJsonFormatProvider;
import io.github.jordepic.streamfusion.format.json.MaxwellJsonFormatProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.formats.json.maxwell.MaxwellJsonDeserializationSchema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Pins the native CDC decode to Flink's own Maxwell/Canal/Debezium deserializers, message by
 * message (no containers — the format classes referee directly, like {@link CsvDecodeParityTest}).
 * The heart of it is the partial-{@code old} pre-image rule that used to gate Maxwell/Canal off the
 * native path: Flink reads an UPDATE_BEFORE field from {@code old} when its KEY is present there —
 * an explicit null means "was null" and stays null; an absent key means "unchanged" and copies the
 * post-image — and for Canal the presence check spans the WHOLE {@code old} array
 * ({@code findValue} over the array node). The native decode reproduces that with a per-message key
 * scan of the raw {@code old} JSON; every scenario here must produce the same changelog (RowKinds
 * included) from both engines, or fail on both.
 */
@Tag("streamfusion-json")
class CdcDecodeParityTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
          new String[] {"id", "name", "score"});

  private static final int MAXWELL = 8;
  private static final int CANAL = 9;
  private static final int DEBEZIUM = 6;

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

  private static void assertParity(int format, String message, boolean skipErrors)
      throws Exception {
    List<List<Object>> expected;
    try {
      expected = flinkDecode(format, message, skipErrors);
    } catch (Exception e) {
      expected = null;
    }
    List<List<Object>> actual;
    try {
      actual = nativeDecode(format, message, skipErrors);
    } catch (Exception e) {
      actual = null;
    }
    if (expected == null) {
      assertNull(actual, "Flink rejects but native decode accepts: " + message);
      return;
    }
    assertNotNull(actual, "Flink accepts but native decode rejects: " + message);
    assertEquals(expected, actual, "changelog diverges for: " + message);
  }

  private static List<List<Object>> flinkDecode(int format, String message, boolean ignoreErrors)
      throws Exception {
    DataType physical = TypeConversions.fromLogicalToDataType(ROW_TYPE);
    InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(ROW_TYPE);
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
            rows.add(fields(row));
          }

          @Override
          public void close() {}
        });
    return rows;
  }

  private static List<List<Object>> nativeDecode(int format, String message, boolean skipErrors)
      throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                ROW_TYPE,
                100,
                provider(format)
                    .createDecoder(
                        new NativeFormatContext(
                            ROW_TYPE,
                            ROW_TYPE,
                            Map.of("format", provider(format).formatIdentifier()),
                            skipErrors)),
                0),
            BytePrimitiveArraySerializer.INSTANCE)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(message.getBytes(StandardCharsets.UTF_8)));
      harness.prepareSnapshotPreBarrier(1L);
      List<List<Object>> rows = new ArrayList<>();
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, ROW_TYPE)) {
              rows.add(fields(row));
            }
          }
        }
      }
      return rows;
    }
  }

  private static NativeFormatProvider provider(int format) {
    return switch (format) {
      case MAXWELL -> new MaxwellJsonFormatProvider();
      case CANAL -> new CanalJsonFormatProvider();
      case DEBEZIUM -> new DebeziumJsonFormatProvider();
      default -> throw new IllegalArgumentException("Unknown CDC format: " + format);
    };
  }

  /** The row's kind plus each field rendered — what parity compares. */
  private static List<Object> fields(RowData row) {
    List<Object> values = new ArrayList<>();
    values.add(row.getRowKind().shortString());
    for (int i = 0; i < ROW_TYPE.getFieldCount(); i++) {
      Object value = RowData.createFieldGetter(ROW_TYPE.getTypeAt(i), i).getFieldOrNull(row);
      values.add(value == null ? null : value.toString());
    }
    return values;
  }
}
