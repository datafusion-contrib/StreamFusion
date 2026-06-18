package io.github.jordepic.streamfusion.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import io.github.jordepic.streamfusion.Native;
import org.junit.jupiter.api.Test;

class IntColumnBridgeTest {

  @Test
  void runsRowsThroughNativeOperatorAsAColumn() {
    int[] values = {3, 4, 5};
    List<RowData> rows = new ArrayList<>();
    for (int value : values) {
      GenericRowData row = new GenericRowData(1);
      row.setField(0, value);
      rows.add(row);
    }

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider provider = new CDataDictionaryProvider();
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator);
        IntVector column = IntColumnBridge.toArrow(rows, 0, allocator)) {

      Data.exportVector(allocator, column, provider, inArray, inSchema);

      Native.doubleColumn(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());

      try (IntVector result =
          (IntVector) Data.importVector(allocator, outArray, outSchema, provider)) {
        List<RowData> back = IntColumnBridge.fromArrow(result);
        assertEquals(values.length, back.size());
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i] * 2, back.get(i).getInt(0));
        }
      }
    }
  }
}
