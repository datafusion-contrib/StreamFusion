package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.jupiter.api.Test;

class ArrowBridgeTest {

  @Test
  void nativeSumsIntColumnExportedThroughCDataInterface() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector vector = new IntVector("c0", allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {

      int[] values = {1, 2, 3, 4, 5};
      vector.allocateNew(values.length);
      for (int i = 0; i < values.length; i++) {
        vector.set(i, values[i]);
      }
      vector.setValueCount(values.length);

      Data.exportVector(allocator, vector, provider, array, schema);

      long sum = Native.sumInt(array.memoryAddress(), schema.memoryAddress());

      assertEquals(15, sum);
    }
  }
}
