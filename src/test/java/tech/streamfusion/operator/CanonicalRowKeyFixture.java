package tech.streamfusion.operator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import tech.streamfusion.arrow.ArrowConversion;

public final class CanonicalRowKeyFixture {
  private CanonicalRowKeyFixture() {}

  public static RowType type(boolean nested) {
    return nested
        ? RowType.of(
            new BigIntType(),
            RowType.of(new IntType(), new VarCharType(100)),
            new TimestampType(9),
            new DecimalType(38, 2))
        : RowType.of(new BigIntType(), new VarCharType(100));
  }

  public static String sql(boolean nested) {
    return nested
        ? "ROW<i BIGINT,n ROW<i INT,s VARCHAR(100)>,t TIMESTAMP(9),d DECIMAL(38,2)>"
        : "ROW<i BIGINT,s VARCHAR(100)>";
  }

  public static RowData key(boolean nested, Long id, RowKind kind, boolean padding) {
    var value = logicalKey(nested, id, kind);
    if (value == null) return null;
    var binary = new RowDataSerializer(type(nested)).toBinaryRow(value).copy();
    if (padding) {
      byte[] bytes =
          BinarySegmentUtils.copyToBytes(
              binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
      bytes = Arrays.copyOf(bytes, bytes.length + 8);
      Arrays.fill(bytes, bytes.length - 8, bytes.length, (byte) 0x55);
      binary.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
    }
    return binary;
  }

  public static RowData logicalKey(boolean nested, Long id, RowKind kind) {
    if (id == null) return null;
    var child =
        GenericRowData.of(id.intValue(), id % 3 == 0 ? null : StringData.fromString("雪" + id));
    child.setRowKind(kind);
    var value =
        nested
            ? GenericRowData.of(
                id,
                id % 2 == 0 ? null : child,
                id % 2 == 0 ? null : TimestampData.fromEpochMillis(-1234 - id, 456789),
                id % 2 == 0
                    ? null
                    : DecimalData.fromBigDecimal(
                        new BigDecimal("123456789012345678901234567890123456.78"), 38, 2))
            : GenericRowData.of(id, id % 2 == 0 ? null : StringData.fromString("雪" + id));
    value.setRowKind(kind);
    return value;
  }

  public static VectorSchemaRoot arrow(List<RowData> rows, RowType type) {
    var root =
        VectorSchemaRoot.create(ArrowConversion.toBinaryRowKeySchema(type), NativeAllocator.SHARED);
    var kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
    try {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (var row : rows) writer.write(row);
      writer.finish();
      kinds.allocateNew(rows.size());
      for (int i = 0; i < rows.size(); i++)
        kinds.setSafe(i, rows.get(i).getRowKind().toByteValue());
      kinds.setValueCount(rows.size());
      var fields = new ArrayList<>(root.getSchema().getFields());
      fields.add(kinds.getField());
      var vectors = new ArrayList<>(root.getFieldVectors());
      vectors.add(kinds);
      return new VectorSchemaRoot(fields, vectors, rows.size());
    } catch (RuntimeException | Error failure) {
      root.close();
      kinds.close();
      throw failure;
    }
  }

  public static List<String> bytes(RowType type, List<RowData> rows) {
    var serializer = new RowDataSerializer(type);
    var result = new ArrayList<String>();
    for (var row : rows) {
      var binary = serializer.toBinaryRow(row);
      result.add(
          java.util.Base64.getEncoder()
              .encodeToString(
                  BinarySegmentUtils.copyToBytes(
                      binary.getSegments(), binary.getOffset(), binary.getSizeInBytes())));
    }
    result.sort(String::compareTo);
    return result;
  }
}
