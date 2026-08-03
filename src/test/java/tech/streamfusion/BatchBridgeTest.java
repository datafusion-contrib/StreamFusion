package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

class BatchBridgeTest {

  @Test
  void echoesMultiColumnBatchThroughCDataInterface() {
    long[] timestamps = {0, 1000, 1500};
    long[] values = {10, 20, 30};

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

      Native.echoBatch(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());

      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, provider)) {
        assertEquals(timestamps.length, result.getRowCount());
        BigIntVector outTs = (BigIntVector) result.getVector("ts");
        BigIntVector outValue = (BigIntVector) result.getVector("value");
        for (int i = 0; i < timestamps.length; i++) {
          assertEquals(timestamps[i], outTs.get(i));
          assertEquals(values[i], outValue.get(i));
        }
      }
    }
  }
}
