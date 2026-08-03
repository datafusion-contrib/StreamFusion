package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Shared plumbing for the per-message decode parity tests: runs one message through the native
 * decode operator, renders each row (its kind too, when the format is a changelog) for comparison,
 * and asserts both engines reach the same outcome — identical rows field for field, or both
 * failing. Each test keeps its own Flink referee and option fixtures.
 */
final class DecodeParityHarness {

  interface Decode {
    List<List<Object>> decode() throws Exception;
  }

  private final RowType rowType;
  private final boolean compareRowKinds;

  DecodeParityHarness(RowType rowType, boolean compareRowKinds) {
    this.rowType = rowType;
    this.compareRowKinds = compareRowKinds;
  }

  void assertParity(String label, Decode flinkDecode, Decode nativeDecode) {
    List<List<Object>> expected;
    try {
      expected = flinkDecode.decode();
    } catch (Exception e) {
      expected = null; // Flink failed the message — the native decode must fail it too
    }
    List<List<Object>> actual;
    try {
      actual = nativeDecode.decode();
    } catch (Exception e) {
      actual = null;
    }
    if (expected == null) {
      assertNull(actual, "Flink rejects but native decode accepts: " + label);
      return;
    }
    assertNotNull(actual, "Flink accepts but native decode rejects: " + label);
    assertEquals(expected, actual, "decoded values diverge for: " + label);
  }

  List<List<Object>> nativeDecode(
      NativeFormatProvider provider,
      String message,
      Map<String, String> formatOptions,
      boolean skipErrors)
      throws Exception {
    return nativeDecode(
        provider, message.getBytes(StandardCharsets.UTF_8), formatOptions, skipErrors);
  }

  List<List<Object>> nativeDecode(
      NativeFormatProvider provider,
      byte[] message,
      Map<String, String> formatOptions,
      boolean skipErrors)
      throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                rowType,
                100,
                provider.createDecoder(
                    new NativeFormatContext(rowType, rowType, formatOptions, skipErrors)),
                0),
            BytePrimitiveArraySerializer.INSTANCE)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(message));
      harness.prepareSnapshotPreBarrier(1L);
      List<List<Object>> rows = new ArrayList<>();
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, rowType)) {
              rows.add(fields(row));
            }
          }
        }
      }
      return rows;
    }
  }

  /** Each field rendered — plus the row's kind when the format is a changelog. */
  List<Object> fields(RowData row) {
    List<Object> values = new ArrayList<>();
    if (compareRowKinds) {
      values.add(row.getRowKind().shortString());
    }
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      LogicalType type = rowType.getTypeAt(i);
      values.add(render(RowData.createFieldGetter(type, i).getFieldOrNull(row), type));
    }
    return values;
  }

  /**
   * A value rendered for comparison across engines. Scalars keep their internal-form
   * {@code toString}; binary compares by content (byte[] has none); nested containers render
   * element-wise (their internal classes differ between engines), maps sorted by key so hash
   * iteration order cannot fail parity.
   */
  private static Object render(Object value, LogicalType type) {
    if (value == null) {
      return null;
    }
    switch (type.getTypeRoot()) {
      case BINARY:
      case VARBINARY:
        return Arrays.toString((byte[]) value);
      case ARRAY:
        return renderArray((ArrayData) value, ((ArrayType) type).getElementType());
      case MAP:
        MapType mapType = (MapType) type;
        return renderMap((MapData) value, mapType.getKeyType(), mapType.getValueType());
      case MULTISET:
        MultisetType multisetType = (MultisetType) type;
        return renderMap((MapData) value, multisetType.getElementType(), new IntType(false));
      case ROW:
        RowData nested = (RowData) value;
        RowType nestedType = (RowType) type;
        List<Object> fields = new ArrayList<>();
        for (int i = 0; i < nestedType.getFieldCount(); i++) {
          LogicalType fieldType = nestedType.getTypeAt(i);
          fields.add(render(RowData.createFieldGetter(fieldType, i).getFieldOrNull(nested), fieldType));
        }
        return fields;
      default:
        return value.toString();
    }
  }

  private static List<Object> renderArray(ArrayData array, LogicalType elementType) {
    ArrayData.ElementGetter getter = ArrayData.createElementGetter(elementType);
    List<Object> elements = new ArrayList<>();
    for (int i = 0; i < array.size(); i++) {
      elements.add(render(getter.getElementOrNull(array, i), elementType));
    }
    return elements;
  }

  private static Object renderMap(MapData map, LogicalType keyType, LogicalType valueType) {
    List<Object> keys = renderArray(map.keyArray(), keyType);
    List<Object> values = renderArray(map.valueArray(), valueType);
    List<String> entries = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      entries.add(keys.get(i) + "=" + values.get(i));
    }
    Collections.sort(entries);
    return entries;
  }
}
