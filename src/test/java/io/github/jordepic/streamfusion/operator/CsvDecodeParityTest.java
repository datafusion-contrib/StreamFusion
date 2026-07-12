package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.csv.CsvFormatProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
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
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Pins the native CSV decode to Flink's own {@code csv} format, message by message: each scenario is
 * decoded by {@link CsvRowDataDeserializationSchema} (the referee) and by the native decode operator,
 * and the outcomes must match — the same rows field for field, or both failing. This is the
 * accept/reject-envelope test the format-option audit called for: trimming, empty fields, null
 * literals, arity, quoting, comments, Flink's lenient DATE, the SQL timestamp shape, HALF_UP decimal
 * rescale with overflow-to-NULL, and ignore-parse-errors' per-field granularity.
 */
@Tag("streamfusion-csv")
class CsvDecodeParityTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new VarCharType(VarCharType.MAX_LENGTH),
            new IntType(),
            new DoubleType(),
            new BooleanType(),
            new DateType(),
            new TimestampType(3),
            new DecimalType(5, 2),
            new BigIntType()
          },
          new String[] {"s", "i", "f", "b", "d", "ts", "dec", "l"});

  private static final String[] DEFAULT_OPTION_SCENARIOS = {
    // Plain row, and Flink's trimming rules: numbers/booleans trim, strings don't.
    "hello,42,2.5,true,2020-01-02,2020-01-02 03:04:05.678,1.23,9",
    " padded , 42 , 2.5 , TRUE ,2020-01-02,  2020-01-02 03:04:05  , 1.23 , 9 ",
    // Empty fields: "" stays a string; an empty number/boolean/date is Flink's per-type behavior.
    ",42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,42,2.5,,2020-01-02,2020-01-02 03:04:05,1.23,9",
    // Quoted values, quoted empty, embedded delimiter/newline/doubled quote.
    "\"a,b\",42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "\"line\nbreak\",42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "\"do\"\"uble\",42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "\"\",42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "\"quoted\",\"42\",\"2.5\",\"true\",2020-01-02,2020-01-02 03:04:05,\"1.23\",9",
    // Numbers: string-encoded, signs, overflow, float specials and suffixes, int-out-of-range.
    "x,+42,-2.5,false,2020-01-02,2020-01-02 03:04:05,-1.23,-9",
    "x,42,Infinity,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,42,-Infinity,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,42,1.5d,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,42,1e999,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,42,inf,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,1.5,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,3000000000,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,junk,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    // Booleans never fail in Flink's converter: anything not "true" is false.
    "x,42,2.5,yes,2020-01-02,2020-01-02 03:04:05,1.23,9",
    "x,42,2.5,1,2020-01-02,2020-01-02 03:04:05,1.23,9",
    // DATE is java.sql.Date.valueOf: single digits and lenient day overflow.
    "x,42,2.5,true,2020-1-2,2020-01-02 03:04:05,1.23,9",
    "x,42,2.5,true,2020-02-31,2020-01-02 03:04:05,1.23,9",
    "x,42,2.5,true,2020-13-01,2020-01-02 03:04:05,1.23,9",
    "x,42,2.5,true,02-01-2020,2020-01-02 03:04:05,1.23,9",
    // TIMESTAMP: SQL form only, fraction digits, T-separator/offsets rejected, trailing dot.
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05.123456789,1.23,9",
    "x,42,2.5,true,2020-01-02,2020-01-02T03:04:05,1.23,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04,1.23,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05.,1.23,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05+05:00,1.23,9",
    "x,42,2.5,true,2020-01-02,2020-01-02,1.23,9",
    // DECIMAL: HALF_UP rounding past the scale, precision overflow, exponents, grammar edges.
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.235,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,-1.235,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,12345.6,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,1e2,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,.5,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.,9",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05, 1.23,9",
    // Arity: short row, long row, lone field.
    "x,42,2.5",
    "x,42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9,extra",
    "x",
    // No usable record at all.
    "",
  };

  @Test
  void defaultOptionsMatchFlinkPerMessage() throws Exception {
    for (String scenario : DEFAULT_OPTION_SCENARIOS) {
      assertParity(scenario, b -> {}, "", false);
      assertParity(scenario, b -> b.setIgnoreParseErrors(true), "", true);
    }
  }

  @Test
  void delimiterQuoteAndCommentsMatchFlink() throws Exception {
    // escape-character is deliberately absent: Jackson unescapes in unquoted fields where csv-core
    // cannot, so a table setting it falls back (see KafkaTables.encodeFormatOptions).
    String[] scenarios = {
      "a;42;2.5;true;2020-01-02;2020-01-02 03:04:05;1.23;9",
      "a,b;42;2.5;true;2020-01-02;2020-01-02 03:04:05;1.23;9",
      "|quo;ted|;42;2.5;true;2020-01-02;2020-01-02 03:04:05;1.23;9",
      "#comment,line;42;2.5;true;2020-01-02;2020-01-02 03:04:05;1.23;9",
    };
    Consumer<CsvRowDataDeserializationSchema.Builder> flink =
        b -> b.setFieldDelimiter(';').setQuoteCharacter('|').setAllowComments(true);
    String nativeOptions =
        "csv.field-delimiter=;\ncsv.quote-character=|\ncsv.allow-comments=true\n";
    for (String scenario : scenarios) {
      assertParity(scenario, flink, nativeOptions, false);
      assertParity(
          scenario, flink.andThen(b -> b.setIgnoreParseErrors(true)), nativeOptions, true);
    }
  }

  @Test
  void disabledQuoteCharacterMatchesFlink() throws Exception {
    assertParity(
        "\"raw,42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
        b -> b.disableQuoteCharacter(),
        "csv.disable-quote-character=true\n",
        false);
  }

  @Test
  void nullLiteralMatchesFlink() throws Exception {
    String[] scenarios = {
      "null,null,null,null,null,null,null,null",
      "x,42, null ,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
      "\"null\",42,2.5,true,2020-01-02,2020-01-02 03:04:05,1.23,9",
    };
    for (String scenario : scenarios) {
      assertParity(
          scenario, b -> b.setNullLiteral("null"), "csv.null-literal=null\n", false);
    }
  }

  /**
   * Decodes one message through both engines and asserts the same outcome: identical rows (field by
   * field, via each type's {@link RowData.FieldGetter}) or both failing.
   */
  private static void assertParity(
      String message,
      Consumer<CsvRowDataDeserializationSchema.Builder> flinkOptions,
      String nativeFormatOptions,
      boolean skipErrors)
      throws Exception {
    List<List<Object>> expected;
    try {
      expected = flinkDecode(message, flinkOptions);
    } catch (Exception e) {
      expected = null; // Flink failed the message — the native decode must fail it too
    }
    List<List<Object>> actual;
    try {
      actual = nativeDecode(message, nativeFormatOptions, skipErrors);
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
      String message, Consumer<CsvRowDataDeserializationSchema.Builder> customize)
      throws Exception {
    CsvRowDataDeserializationSchema.Builder builder =
        new CsvRowDataDeserializationSchema.Builder(ROW_TYPE, InternalTypeInfo.of(ROW_TYPE));
    customize.accept(builder);
    CsvRowDataDeserializationSchema schema = builder.build();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(CsvDecodeParityTest.class.getClassLoader());
          }
        });
    RowData row = schema.deserialize(message.getBytes(StandardCharsets.UTF_8));
    return row == null ? List.of() : List.of(fields(row));
  }

  private static List<List<Object>> nativeDecode(
      String message, String formatOptions, boolean skipErrors) throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                ROW_TYPE,
                100,
                new CsvFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            ROW_TYPE, ROW_TYPE, nativeOptions(formatOptions), skipErrors)),
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

  private static Map<String, String> nativeOptions(String encoded) {
    Map<String, String> options = new HashMap<>();
    options.put("format", "csv");
    for (String line : encoded.split("\\n")) {
      if (line.isEmpty()) {
        continue;
      }
      int separator = line.indexOf('=');
      options.put(line.substring(0, separator), line.substring(separator + 1));
    }
    return options;
  }

  private static List<Object> fields(RowData row) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < ROW_TYPE.getFieldCount(); i++) {
      Object value = RowData.createFieldGetter(ROW_TYPE.getTypeAt(i), i).getFieldOrNull(row);
      values.add(value == null ? null : value.toString());
    }
    return values;
  }
}
