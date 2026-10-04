package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FixedBinaryInitializationTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 16, 256})
  void writesOwnBytesAndNullsAcrossGrowthWhilePriorBuffersRemainRetained(int width) {
    RowType type = RowType.of(new BinaryType(width));
    try (var allocator = new RootAllocator();
         var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type, 2);
      var vector = (FixedSizeBinaryVector) root.getVector(0);
      vector.getDataBuffer().setByte(0, 0x55);
      assertTrue(vector.isNull(0));
      byte[] reused = new byte[width];
      for (int i = 0; i < 65; i++) {
        Arrays.fill(reused, (byte) (i + 128));
        writer.write(GenericRowData.of(i % 7 == 0 ? null : reused));
        Arrays.fill(reused, (byte) 0x55);
      }
      writer.finish();
      for (int i = 0; i < 65; i++) {
        if (i % 7 == 0) {
          assertTrue(vector.isNull(i));
          assertTrue(ArrowConversion.createArrowReader(root, type).read(i).isNullAt(0));
        }
        else {
          byte[] expected = new byte[width];
          Arrays.fill(expected, (byte) (i + 128));
          assertArrayEquals(expected, vector.get(i));
        }
      }
      var priorData = vector.getDataBuffer();
      var priorValidity = vector.getValidityBuffer();
      priorData.getReferenceManager().retain();
      priorValidity.getReferenceManager().retain();
      try {
        long address = priorData.memoryAddress();
        var next = ArrowConversion.createRowDataArrowWriter(root, type, 2);
        assertNotEquals(address, vector.getDataBuffer().memoryAddress());
        next.write(GenericRowData.of((Object) null));
        next.write(GenericRowData.of((Object) null));
        next.finish();
        assertTrue(vector.isNull(0));
        assertTrue(vector.isNull(1));
        var reader = ArrowConversion.createArrowReader(root, type);
        assertTrue(reader.read(0).isNullAt(0));
        assertTrue(reader.read(1).isNullAt(0));
        assertEquals((byte) 129, priorData.getByte(width));
        assertEquals(0, priorValidity.getByte(0) & 1);
        assertNotEquals(0, priorValidity.getByte(0) & 2);
      } finally {
        priorValidity.close();
        priorData.close();
      }
    }
  }
}
