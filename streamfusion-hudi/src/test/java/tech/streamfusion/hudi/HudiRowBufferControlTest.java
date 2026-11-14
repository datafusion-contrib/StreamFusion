package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hudi.sink.buffer.HeapMemorySegmentPool;
import org.apache.hudi.sink.utils.BufferUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

class HudiRowBufferControlTest {
  @BeforeAll
  static void memory() {
    var conf = new Configuration();
    conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    TaskOffHeapMemory.initialize(conf);
  }

  @Test
  void arrowOffsetLengthsMatchFlinkRowsAcrossInlineAndNullBitsetBoundaries() {
    for (int fields : new int[] {3, 70}) {
      var type = type(fields);
      List<RowData> values = new ArrayList<>();
      for (int length : new int[] {0, 1, 7, 8, 9, 15, 16, 17, 32700}) {
        values.add(row(fields, "x".repeat(length)));
        values.add(row(fields, "\u00e9".repeat(length)));
      }
      values.add(row(fields, null));
      var serializer = new RowDataSerializer(type);
      try (var root = RowDataArrowConverter.write(values, type, NativeAllocator.SHARED)) {
        for (int index = 0; index < values.size(); index++)
          assertEquals(
              serializer.toBinaryRow(values.get(index)).getSizeInBytes(),
              HudiRowBufferControl.rowBytes(root, index, type));
      }
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void scalarSizesMatchFlinkWithoutSerializingArrowPayloads() {
    var type =
        RowType.of(
            new org.apache.flink.table.types.logical.VarBinaryType(),
            new org.apache.flink.table.types.logical.DecimalType(25, 6),
            new org.apache.flink.table.types.logical.DecimalType(9, 3),
            new org.apache.flink.table.types.logical.TimestampType(6),
            new org.apache.flink.table.types.logical.LocalZonedTimestampType(6));
    List<RowData> values = new ArrayList<>();
    values.add(new GenericRowData(5));
    for (int length : new int[] {0, 1, 7, 8, 15, 16, 32769}) {
      values.add(
          GenericRowData.of(
              new byte[length],
              org.apache.flink.table.data.DecimalData.fromBigDecimal(
                  new java.math.BigDecimal("1234567890123456789.123456"), 25, 6),
              org.apache.flink.table.data.DecimalData.fromBigDecimal(
                  new java.math.BigDecimal("-12.345"), 9, 3),
              org.apache.flink.table.data.TimestampData.fromEpochMillis(-1, 123000),
              org.apache.flink.table.data.TimestampData.fromEpochMillis(123, 456000)));
    }
    var serializer = new RowDataSerializer(type);
    try (var root = RowDataArrowConverter.write(values, type, NativeAllocator.SHARED)) {
      for (int row = 0; row < values.size(); row++)
        assertEquals(
            serializer.toBinaryRow(values.get(row)).getSizeInBytes(),
            HudiRowBufferControl.rowBytes(root, row, type));
    }
  }

  @Test
  void skippedPayloadsUseTheSamePagesCapacityAndFailureBoundaryAsTheStockBuffer() throws Exception {
    var type = type(3);
    var stockPool = new HeapMemorySegmentPool(32768, 1 << 20);
    var controlPool = new HeapMemorySegmentPool(32768, 1 << 20);
    var stock = BufferUtils.createBuffer(type, stockPool);
    var control = HudiRowBufferControl.create(type, controlPool);
    var serializer = new RowDataSerializer(type);
    var shape = HudiRowBufferControl.shape(3);
    boolean exhausted = false;
    try {
      int[] lengths = {0, 7, 8, 30000, 1, 32700, 40, 65000};
      for (int index = 0; index < 1000; index++) {
        var binary = serializer.toBinaryRow(row(3, "x".repeat(lengths[index % lengths.length])));
        shape.setTotalSize(binary.getSizeInBytes());
        boolean accepted = stock.write(binary);
        assertEquals(accepted, control.write(shape), "same admission at row " + index);
        assertEquals(stockPool.freePages(), controlPool.freePages());
        assertEquals(stock.getOccupancy(), control.getOccupancy());
        if (!accepted) {
          exhausted = true;
          break;
        }
      }
      assertTrue(exhausted, "The oracle must reach the actual pool limit");
    } finally {
      stock.dispose();
      control.dispose();
    }
    assertEquals(32, stockPool.freePages());
    assertEquals(stockPool.freePages(), controlPool.freePages());
  }

  private static RowType type(int count) {
    var fields = new ArrayList<RowType.RowField>();
    for (int column = 0; column < count; column++)
      fields.add(
          new RowType.RowField(
              "f" + column,
              column == 1
                  ? new VarCharType(VarCharType.MAX_LENGTH)
                  : column == count - 1 ? new BooleanType() : new IntType()));
    return new RowType(false, fields);
  }

  private static GenericRowData row(int fields, String string) {
    var row = new GenericRowData(fields);
    for (int column = 0; column < fields; column++) {
      if (column == 1) row.setField(column, string == null ? null : StringData.fromString(string));
      else if (column == fields - 1) row.setField(column, true);
      else if (column % 3 != 0) row.setField(column, column);
    }
    return row;
  }
}
