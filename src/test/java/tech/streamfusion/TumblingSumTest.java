package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class TumblingSumTest {

  @Test
  void sumsValuesPerEventTimeWindow() {
    long[] timestamps = {0, 500, 1000, 1500, 2500};
    long[] values = {1, 2, 3, 4, 5};
    long window = 1000;

    try (BufferAllocator allocator = new RootAllocator();
        BigIntVector ts = new BigIntVector("ts", allocator);
        BigIntVector value = new BigIntVector("value", allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider();
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {

      ts.allocateNew(timestamps.length);
      value.allocateNew(values.length);
      for (int i = 0; i < timestamps.length; i++) {
        ts.set(i, timestamps[i]);
        value.set(i, values[i]);
      }
      ts.setValueCount(timestamps.length);
      value.setValueCount(values.length);

      try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(ts, value))) {
        root.setRowCount(timestamps.length);
        Data.exportVectorSchemaRoot(allocator, root, provider, inArray, inSchema);
      }

      Native.tumblingSum(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          window,
          outArray.memoryAddress(),
          outSchema.memoryAddress());

      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, provider)) {
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector total = (BigIntVector) result.getVector("total");

        List<Long> starts = new ArrayList<>();
        List<Long> totals = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
          starts.add(windowStart.get(i));
          totals.add(total.get(i));
        }

        assertEquals(List.of(0L, 1000L, 2000L), starts);
        assertEquals(List.of(3L, 7L, 5L), totals);
      }
    }
  }
}
