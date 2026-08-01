package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.json.JsonFormatProvider;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchSerializer;
import io.github.jordepic.streamfusion.operator.NativeBytesDecodeOperator;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The keyed composition through the real operator and native JSON decoder (so it runs in the
 * suites whose native library compiles the json feature): the raw key fills its physical position
 * for every row its record's value produced — a JSON top-level array fans out N rows sharing the
 * key, a NULL Kafka key keeps the record with a NULL key column (raw's null-key rule), and a
 * record dropped under ignore-parse-errors contributes nothing, key and all.
 */
@Tag("streamfusion-json")
class KeyedDecodeOperatorTest {

  private static final RowType PHYSICAL =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()
          },
          new String[] {"id", "name", "k"});

  @Test
  void framesDecodeThroughTheKeyedOperator() throws Exception {
    Map<String, String> options =
        Map.of(
            "connector", "kafka",
            "format", "json",
            "json.ignore-parse-errors", "true",
            "key.format", "raw",
            "key.fields", "k",
            "value.fields-include", "EXCEPT_KEY");
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(options, PHYSICAL);
    assertNotNull(spec);

    JsonFormatProvider provider = new JsonFormatProvider();
    NativeFormatContext context =
        new NativeFormatContext(
            spec.valueRowType(), spec.valueRowType(), spec.optionsWithMarkers(), true);
    assertTrue(provider.supports(context));

    List<byte[]> frames =
        List.of(
            frame(7L, "{\"id\": 1, \"name\": \"a\"}"),
            // A top-level array fans out into two rows sharing record 1's key.
            frame(8L, "[{\"id\": 2, \"name\": \"b\"}, {\"id\": 3, \"name\": \"c\"}]"),
            // A null Kafka key keeps the record with a NULL key column.
            NativeBytesDecodeOperator.frame(
                null, "{\"id\": 4, \"name\": \"d\"}".getBytes(StandardCharsets.UTF_8)),
            // A malformed body drops the whole record under ignore-parse-errors.
            frame(9L, "not json"));

    List<List<Object>> rows = new ArrayList<>();
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                PHYSICAL, 100, provider.createDecoder(context), 0, true),
            BytePrimitiveArraySerializer.INSTANCE)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      for (byte[] frame : frames) {
        harness.processElement(new StreamRecord<>(frame));
      }
      harness.prepareSnapshotPreBarrier(1L);
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, PHYSICAL)) {
              rows.add(
                  List.of(
                      String.valueOf(row.isNullAt(0) ? null : row.getLong(0)),
                      String.valueOf(row.isNullAt(1) ? null : row.getString(1)),
                      String.valueOf(row.isNullAt(2) ? null : row.getLong(2))));
            }
          }
        }
      }
    }

    assertEquals(
        List.of(
            List.of("1", "a", "7"),
            List.of("2", "b", "8"),
            List.of("3", "c", "8"),
            List.of("4", "d", "null")),
        rows);
  }

  private static byte[] frame(long key, String value) {
    byte[] keyBytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      keyBytes[i] = (byte) (key >>> (56 - 8 * i));
    }
    return NativeBytesDecodeOperator.frame(keyBytes, value.getBytes(StandardCharsets.UTF_8));
  }
}
