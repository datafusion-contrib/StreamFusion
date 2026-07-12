package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.json.JsonFormatProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonParserRowDataDeserializationSchema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Pins the native JSON decode to Flink's own default deserializer
 * ({@link JsonParserRowDataDeserializationSchema}), message by message, the way the CSV audit's
 * {@link CsvDecodeParityTest} does: both engines decode each scenario and the outcomes must match —
 * the same rows field for field, or both failing. Covers Flink's scalar coercions (string-encoded
 * numbers with trimming, {@code Infinity}/{@code NaN}/suffix floats, never-failing booleans,
 * number/boolean/container echo under STRING), the strict {@code ISO_LOCAL_DATE}, both
 * {@code timestamp-format.standard} modes, and — on the decimal-bearing (raw-literal) path — the
 * exact {@code BigDecimal} + HALF_UP-or-NULL decimal semantics.
 *
 * <p>The deliberate residual leniencies of divergences/21 (a trailing 'Z' tolerated on any
 * timestamp column, a float token under a STRING column failing loudly, Unicode-whitespace
 * trimming) are excluded from the corpus by design.
 */
@Tag("streamfusion-json")
class JsonDecodeParityTest {

  private static final RowType SCALAR_TYPE =
      RowType.of(
          new LogicalType[] {
            new VarCharType(VarCharType.MAX_LENGTH),
            new IntType(),
            new DoubleType(),
            new BooleanType(),
            new DateType(),
            new TimestampType(3)
          },
          new String[] {"s", "i", "f", "b", "d", "ts"});

  private static final RowType DECIMAL_TYPE =
      RowType.of(
          new LogicalType[] {new DecimalType(5, 2), new DecimalType(38, 18), new BigIntType()},
          new String[] {"dec", "wide", "l"});

  private static final String TS = "\"ts\": \"2020-01-02 03:04:05.678\"";

  private static final String[] SQL_MODE_SCENARIOS = {
    // Plain row; missing fields and explicit nulls are both SQL NULL.
    "{\"s\": \"x\", \"i\": 42, \"f\": 2.5, \"b\": true, \"d\": \"2020-01-02\", " + TS + "}",
    "{\"s\": null, \"i\": null}",
    "{}",
    // Scalar coercions: string-encoded numbers trim; floats truncate into int columns; booleans
    // never fail; ints/booleans/containers echo under STRING.
    "{\"i\": \" 42 \", \"f\": \" 2.5 \"}",
    "{\"i\": 1.9}",
    "{\"i\": \"1.5\"}",
    "{\"i\": 3000000000}",
    "{\"i\": \"junk\"}",
    "{\"i\": true}",
    "{\"f\": \"Infinity\"}",
    "{\"f\": \"-Infinity\"}",
    "{\"f\": \"NaN\"}",
    "{\"f\": \"1.5d\"}",
    "{\"f\": \"1e999\"}",
    "{\"f\": \"inf\"}",
    "{\"f\": 3}",
    "{\"b\": \"TRUE\"}",
    "{\"b\": \"yes\"}",
    "{\"b\": 1}",
    "{\"s\": 42}",
    "{\"s\": true}",
    "{\"s\": {\"a\": 1, \"b\": [true, null, \"x\\n\"]}}",
    // DATE is the strict ISO_LOCAL_DATE.
    "{\"d\": \"2020-1-2\"}",
    "{\"d\": \"2020-02-30\"}",
    "{\"d\": 42}",
    "{\"d\": \"2020-01-02T00:00:00\"}",
    // TIMESTAMP, SQL standard: space separator, seconds required, 0-9 fraction digits, no offsets,
    // no bare numbers.
    "{\"ts\": \"2020-01-02 03:04:05\"}",
    "{\"ts\": \"2020-01-02 03:04:05.123456789\"}",
    "{\"ts\": \"2020-01-02 03:04:05.\"}",
    "{\"ts\": \"2020-01-02T03:04:05\"}",
    "{\"ts\": \"2020-01-02 03:04\"}",
    "{\"ts\": \"2020-01-02 03:04:05+05:00\"}",
    "{\"ts\": 123456789}",
    // Malformed document.
    "{\"i\": }",
  };

  @Test
  void sqlModeMatchesFlinkPerMessage() throws Exception {
    for (String scenario : SQL_MODE_SCENARIOS) {
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", false);
      assertParity(SCALAR_TYPE, scenario, TimestampFormat.SQL, "", true);
    }
  }

  @Test
  void iso8601ModeMatchesFlinkPerMessage() throws Exception {
    String[] scenarios = {
      "{\"ts\": \"2020-01-02T03:04:05.678\"}",
      "{\"ts\": \"2020-01-02T03:04\"}", // ISO_LOCAL_DATE_TIME: seconds optional
      "{\"ts\": \"2020-01-02 03:04:05\"}", // the SQL shape is rejected in ISO mode
    };
    for (String scenario : scenarios) {
      assertParity(
          SCALAR_TYPE, scenario, TimestampFormat.ISO_8601, "timestamp-format=ISO-8601\n", false);
    }
  }

  @Test
  void decimalPathMatchesFlinkExactly() throws Exception {
    String[] scenarios = {
      // HALF_UP past the declared scale — arrow-json's own parse would truncate.
      "{\"dec\": 1.235, \"wide\": 0.1234567890123456789012345, \"l\": 9}",
      "{\"dec\": -1.235}",
      // Precision overflow is NULL, not an error.
      "{\"dec\": 12345.6}",
      // String-encoded decimals trim; exponents follow BigDecimal.
      "{\"dec\": \" 1.235 \", \"wide\": \"1e-18\"}",
      "{\"dec\": \"junk\"}",
      // The raw literal survives f64-impossible precision.
      "{\"wide\": 0.123456789012345678901234567890123456}",
    };
    for (String scenario : scenarios) {
      assertParity(DECIMAL_TYPE, scenario, TimestampFormat.SQL, "", false);
    }
    // ignore-parse-errors on the decimal path: a bad decimal cell nulls per field, like the host.
    assertParity(DECIMAL_TYPE, "{\"dec\": \"junk\", \"l\": 9}", TimestampFormat.SQL, "", true);
  }

  private static void assertParity(
      RowType rowType,
      String message,
      TimestampFormat timestampFormat,
      String nativeFormatOptions,
      boolean skipErrors)
      throws Exception {
    List<List<Object>> expected;
    try {
      expected = flinkDecode(rowType, message, timestampFormat, skipErrors);
    } catch (Exception e) {
      expected = null;
    }
    List<List<Object>> actual;
    try {
      actual = nativeDecode(rowType, message, nativeFormatOptions, skipErrors);
    } catch (Exception e) {
      actual = null;
    }
    if (expected == null) {
      assertNull(actual, "Flink rejects but native decode accepts: " + message);
      return;
    }
    assertNotNull(actual, "Flink accepts but native decode rejects: " + message);
    assertEquals(expected, actual, "decoded values diverge for: " + message);
  }

  private static List<List<Object>> flinkDecode(
      RowType rowType, String message, TimestampFormat timestampFormat, boolean ignoreErrors)
      throws Exception {
    JsonParserRowDataDeserializationSchema schema =
        new JsonParserRowDataDeserializationSchema(
            rowType, InternalTypeInfo.of(rowType), false, ignoreErrors, timestampFormat);
    schema.open(null);
    RowData row = schema.deserialize(message.getBytes(StandardCharsets.UTF_8));
    return row == null ? List.of() : List.of(fields(rowType, row));
  }

  private static List<List<Object>> nativeDecode(
      RowType rowType, String message, String formatOptions, boolean skipErrors) throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                rowType,
                100,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            rowType, rowType, nativeOptions(formatOptions), skipErrors)),
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
            for (RowData row : RowDataArrowConverter.read(root, rowType)) {
              rows.add(fields(rowType, row));
            }
          }
        }
      }
      return rows;
    }
  }

  private static Map<String, String> nativeOptions(String encoded) {
    if (encoded.isEmpty()) {
      return Map.of("format", "json");
    }
    if ("timestamp-format=ISO-8601\n".equals(encoded)) {
      return Map.of("format", "json", "json.timestamp-format.standard", "ISO-8601");
    }
    throw new IllegalArgumentException("Unknown JSON option fixture: " + encoded);
  }

  private static List<Object> fields(RowType rowType, RowData row) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      Object value = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
      values.add(value == null ? null : value.toString());
    }
    return values;
  }
}
