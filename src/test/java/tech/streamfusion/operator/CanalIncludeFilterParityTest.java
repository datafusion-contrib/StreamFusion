package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
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
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.json.CanalJsonFormatProvider;

/**
 * Pins Canal's {@code database.include} / {@code table.include} to Flink's own deserializer,
 * message by message. Flink matches {@code root.get(key).asText()} with
 * {@code Pattern.matcher(text).matches()} after parsing and before converting the row, so a
 * rejected message is dropped even when its data or type is bad, while an unparseable message, a
 * non-object root or a filtered key missing from the root is corrupt.
 */
@Tag("streamfusion-json")
class CanalIncludeFilterParityTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
          },
          new String[] {"id", "name", "score"});

  private static final RowType DECIMAL_ROW_TYPE =
      RowType.of(
          new LogicalType[] {new BigIntType(), new DecimalType(10, 2)},
          new String[] {"id", "amount"});

  private static final String ROW = "{\"id\":1,\"name\":\"a\",\"score\":1.5}";

  @Test
  void tablePatternsMatchLikeJavaRegex() throws Exception {
    String[][] cases = {
      {"orders", "orders"},
      {"orders", "orders_2024"},
      {"orders", "my_orders"},
      {"orders", "ORDERS"},
      {"(?i)orders", "ORDERS"},
      {"^orders$", "orders"},
      {"orders|users", "users"},
      {"orders|users", "items"},
      {"order_\\d+", "order_17"},
      {"order_\\d+", "order_"},
      {"order_[0-9]{2}", "order_123"},
      {"ord.rs", "ord\\u0065rs"},
      {".*", ""},
      {"", ""},
      {"", "orders"},
      {"订单_\\p{IsHan}", "订单_表"},
      {"a.b", "a\\nb"},
      {"(?s)a.b", "a\\nb"},
    };
    for (String[] c : cases) {
      String message =
          "{\"data\":[" + ROW + "],\"database\":\"shop\",\"table\":\"" + c[1] + "\",\"type\":\"INSERT\"}";
      assertParity(ROW_TYPE, null, c[0], message);
    }
  }

  @Test
  void rejectedMessagesNeverFailOnTheirContent() throws Exception {
    String[] messages = {
      // Bad data and unknown types in rejected messages are never converted.
      "{\"data\":[{\"id\":\"x\",\"name\":\"b\",\"score\":\"y\"}],\"table\":\"users\",\"type\":\"INSERT\"}",
      "{\"data\":null,\"table\":\"users\",\"type\":\"UPDATE\"}",
      // TiCDC's canal-json emits DDL as QUERY and watermarks with an empty table.
      "{\"id\":0,\"database\":\"test\",\"table\":\"\",\"pkNames\":null,\"isDdl\":true,"
          + "\"type\":\"QUERY\",\"es\":1639633094670,\"ts\":1639633095489,"
          + "\"sql\":\"drop database if exists test\",\"sqlType\":null,\"mysqlType\":null,"
          + "\"data\":null,\"old\":null,\"_tidb\":{\"commitTs\":429918007904436226}}",
      "{\"id\":0,\"database\":\"\",\"table\":\"\",\"type\":\"TIDB_WATERMARK\","
          + "\"_tidb\":{\"watermarkTs\":429918007904436226}}",
      // The admitted counterparts decode or fail as usual.
      "{\"data\":[" + ROW + "],\"table\":\"orders\",\"type\":\"INSERT\"}",
      "{\"data\":[{\"id\":\"x\",\"name\":\"b\",\"score\":\"y\"}],\"table\":\"orders\",\"type\":\"INSERT\"}",
      "{\"data\":null,\"table\":\"orders\",\"type\":\"QUERY\"}",
    };
    for (String message : messages) {
      assertParity(ROW_TYPE, null, "orders", message);
    }
  }

  @Test
  void unreadableMessagesAreCorrupt() throws Exception {
    String[] messages = {
      "{\"data\":[" + ROW + "],\"type\":\"INSERT\"}",
      "[{\"data\":[" + ROW + "],\"table\":\"orders\",\"type\":\"INSERT\"}]",
      "\"orders\"",
      "42",
      "{\"data\":[" + ROW + "],\"table\":\"orders\",\"type\":\"INSERT\"",
      "   ",
    };
    for (String message : messages) {
      assertParity(ROW_TYPE, null, "orders", message);
    }
  }

  @Test
  void valuesAreReadAsJacksonAsText() throws Exception {
    String[][] cases = {
      {"null", "null"},
      {"null", "\"null\""},
      {"true", "true"},
      {"", "{}"},
      {"", "[\"orders\"]"},
      {"orders", "\"users\",\"table\":\"orders\""},
      {"1.5", "1.50"},
      {"1.50", "1.50"},
      {"100.0", "1e2"},
      {"1E\\+2", "1e2"},
      {"0", "-0"},
      {"12345678901234567890", "12345678901234567890"},
      {"1.0E10", "1.0E10"},
      {"1.0E10", "10000000000.0"},
    };
    for (String[] c : cases) {
      String message = "{\"data\":[" + ROW + "],\"table\":" + c[1] + ",\"type\":\"INSERT\"}";
      assertParity(ROW_TYPE, null, c[0], message);
      String decimalMessage =
          "{\"data\":[{\"id\":1,\"amount\":2.5}],\"table\":" + c[1] + ",\"type\":\"INSERT\"}";
      assertParity(DECIMAL_ROW_TYPE, null, c[0], decimalMessage);
    }
  }

  @Test
  void loneSurrogatesAreSpelledLikeJackson() throws Exception {
    // The native parser reads a lone high surrogate escape as U+0000 where Jackson keeps the
    // surrogate. (A DECIMAL column's envelope decode rejects such a message outright, the
    // documented residual of divergences/21.)
    String message = "{\"data\":[" + ROW + "],\"table\":\"shop\\ud800\",\"type\":\"INSERT\"}";

    assertParity(ROW_TYPE, null, "shop\\p{Cs}", message);
    assertParity(ROW_TYPE, null, "[^\\x00]*", message);
  }

  @Test
  void numberSpellingFollowsEveryValueColumnNotTheProjection() throws Exception {
    // Flink's Kafka source never pushes a projection into the format, so its mapper reads floats as
    // BigDecimal whenever the table has a DECIMAL column, selected or not. The planner never
    // narrows a CDC decode today; this pins the decoder's defensive choice should that change.
    String message = "{\"data\":[{\"id\":1,\"amount\":2.5}],\"table\":1e2,\"type\":\"INSERT\"}";
    CanalJsonDeserializationSchema flink =
        CanalJsonDeserializationSchema.builder(
                TypeConversions.fromLogicalToDataType(DECIMAL_ROW_TYPE),
                List.of(),
                InternalTypeInfo.of(DECIMAL_ROW_TYPE))
            .setTable("1E\\+2")
            .build();
    flink.open(null);
    List<RowData> flinkRows = new ArrayList<>();
    flink.deserialize(
        message.getBytes(StandardCharsets.UTF_8),
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            flinkRows.add(row);
          }

          @Override
          public void close() {}
        });
    RowType projected = RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});
    Map<String, String> options =
        Map.of("format", "canal-json", "canal-json.table.include", "1E\\+2");
    long nativeRows = 0;
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                projected,
                100,
                new CanalJsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(projected, DECIMAL_ROW_TYPE, options, false)),
                0),
            BytePrimitiveArraySerializer.INSTANCE)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(message.getBytes(StandardCharsets.UTF_8)));
      harness.prepareSnapshotPreBarrier(1L);
      for (Object event : harness.getOutput()) {
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            nativeRows += root.getRowCount();
          }
        }
      }
    }

    assertEquals(1, flinkRows.size());
    assertEquals(flinkRows.size(), nativeRows);
  }

  @Test
  void filteringDecodersStayJvmMediated() throws Exception {
    Map<String, String> filtered =
        Map.of("format", "canal-json", "canal-json.table.include", "orders");
    Map<String, String> unfiltered = Map.of("format", "canal-json");

    try (NativeMessageDecoder withFilter =
            new CanalJsonFormatProvider()
                .createDecoder(new NativeFormatContext(ROW_TYPE, ROW_TYPE, filtered, false))
                .create();
        NativeMessageDecoder withoutFilter =
            new CanalJsonFormatProvider()
                .createDecoder(new NativeFormatContext(ROW_TYPE, ROW_TYPE, unfiltered, false))
                .create()) {
      assertEquals(0, withFilter.driverInitAddress());
      assertNotEquals(0, withoutFilter.driverInitAddress());
    }
  }

  @Test
  void databaseIsCheckedBeforeTable() throws Exception {
    String[] messages = {
      "{\"data\":[" + ROW + "],\"database\":\"shop\",\"table\":\"orders\",\"type\":\"INSERT\"}",
      "{\"data\":[" + ROW + "],\"database\":\"other\",\"type\":\"INSERT\"}",
      "{\"data\":[" + ROW + "],\"database\":\"shop\",\"type\":\"INSERT\"}",
      "{\"data\":[" + ROW + "],\"table\":\"orders\",\"type\":\"INSERT\"}",
      "{\"data\":[" + ROW + "],\"database\":\"shop\",\"table\":\"users\",\"type\":\"INSERT\"}",
    };
    for (String message : messages) {
      assertParity(ROW_TYPE, "shop", "orders", message);
      assertParity(ROW_TYPE, "shop", null, message);
    }
  }

  private static void assertParity(
      RowType rowType, String database, String table, String message) throws Exception {
    DecodeParityHarness harness = new DecodeParityHarness(rowType, true);
    Map<String, String> options = new HashMap<>();
    options.put("format", "canal-json");
    if (database != null) {
      options.put("canal-json.database.include", database);
    }
    if (table != null) {
      options.put("canal-json.table.include", table);
    }
    for (boolean skipErrors : new boolean[] {false, true}) {
      String label = database + " / " + table + " / skip=" + skipErrors + " : " + message;
      harness.assertParity(
          label,
          () -> {
            CanalJsonDeserializationSchema.Builder builder =
                CanalJsonDeserializationSchema.builder(
                        TypeConversions.fromLogicalToDataType(rowType),
                        List.of(),
                        InternalTypeInfo.of(rowType))
                    .setIgnoreParseErrors(skipErrors);
            if (database != null) {
              builder.setDatabase(database);
            }
            if (table != null) {
              builder.setTable(table);
            }
            CanalJsonDeserializationSchema schema = builder.build();
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
          },
          () -> harness.nativeDecode(new CanalJsonFormatProvider(), message, options, skipErrors));
    }
  }
}
