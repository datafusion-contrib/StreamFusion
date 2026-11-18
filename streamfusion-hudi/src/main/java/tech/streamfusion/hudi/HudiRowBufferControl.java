package tech.streamfusion.hudi;

import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryFormat;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.sink.utils.BufferUtils;

/** Reuses Flink's actual page geometry and Hudi's detector without storing payload bytes. */
final class HudiRowBufferControl {
  private static final Field INPUT_SERIALIZER = serializerField();

  private HudiRowBufferControl() {}

  static void validate() {}

  static BinaryInMemorySortBuffer create(RowType type, MemorySegmentPool pool) {
    var buffer = BufferUtils.createBuffer(type, pool);
    try {
      INPUT_SERIALIZER.set(buffer, new ControlSerializer(type));
      return buffer;
    } catch (ReflectiveOperationException failure) {
      buffer.dispose();
      throw new IllegalStateException("Unsupported Flink buffer serializer boundary", failure);
    }
  }

  static BinaryRowData shape(int fields) {
    var row = new BinaryRowData(fields);
    row.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 0);
    return row;
  }

  static int rowBytes(VectorSchemaRoot root, int row, RowType type) {
    int size = BinaryRowData.calculateFixPartSizeInBytes(type.getFieldCount());
    for (int column = 0; column < type.getFieldCount(); column++) {
      var logical = type.getTypeAt(column);
      var vector = root.getVector(column);
      if (vector.isNull(row)) continue;
      if (vector instanceof BaseVariableWidthVector) {
        int length = ((BaseVariableWidthVector) vector).getValueLength(row);
        if (length > BinaryFormat.MAX_FIX_PART_DATA_SIZE)
          size = Math.addExact(size, Math.addExact(length, 7) & ~7);
      } else if (logical instanceof org.apache.flink.table.types.logical.DecimalType) {
        if (((org.apache.flink.table.types.logical.DecimalType) logical).getPrecision() > 18)
          size = Math.addExact(size, 16);
      } else if (logical instanceof org.apache.flink.table.types.logical.TimestampType) {
        if (((org.apache.flink.table.types.logical.TimestampType) logical).getPrecision() > 3)
          size = Math.addExact(size, 8);
      } else if (logical instanceof org.apache.flink.table.types.logical.LocalZonedTimestampType) {
        if (((org.apache.flink.table.types.logical.LocalZonedTimestampType) logical).getPrecision()
            > 3) size = Math.addExact(size, 8);
      }
    }
    return size;
  }

  private static Field serializerField() {
    try {
      var field = BinaryInMemorySortBuffer.class.getDeclaredField("inputSerializer");
      field.setAccessible(true);
      return field;
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Unsupported Flink paged buffer", failure);
    }
  }

  private static final class ControlSerializer extends RowDataSerializer {
    private final BinaryRowDataSerializer layout;
    private final BinaryRowData empty;

    ControlSerializer(RowType type) {
      super(type);
      layout = new BinaryRowDataSerializer(type.getFieldCount());
      empty = shape(type.getFieldCount());
    }

    @Override
    public int serializeToPages(RowData row, AbstractPagedOutputView output) throws IOException {
      // The SDK still aligns fixed parts and allocates pages; the row body is never materialized.
      int skipped = layout.serializeToPages(empty, output);
      output.skipBytesToWrite(((BinaryRowData) row).getSizeInBytes());
      return skipped;
    }
  }
}
