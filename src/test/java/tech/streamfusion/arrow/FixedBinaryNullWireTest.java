package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FixedBinaryNullWireTest {
  private static final class PoisonedAllocator extends RootAllocator {
    @Override
    public ArrowBuf buffer(long size) {
      ArrowBuf buffer = super.buffer(size);
      byte[] poison = new byte[Math.toIntExact(buffer.capacity())];
      Arrays.fill(poison, (byte) 0x55);
      buffer.setBytes(0, poison);
      return buffer;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 16, 256})
  void serializedNullSlotsRemainZeroWithDirtyFreshAllocations(int width) throws Exception {
    RowType type = RowType.of(new BinaryType(width));
    try (var allocator = new PoisonedAllocator();
         var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type, 8);
      byte[] value = new byte[width];
      Arrays.fill(value, (byte) 0x80);
      for (int i = 0; i < 17; i++) {
        writer.write(GenericRowData.of(i % 2 == 0 ? null : value));
      }
      writer.finish();
      var encoded = new ByteArrayOutputStream();
      try (var stream = new ArrowStreamWriter(root, null, encoded)) {
        stream.start();
        stream.writeBatch();
        stream.end();
      }
      try (var decoded = new ArrowStreamReader(
          new ByteArrayInputStream(encoded.toByteArray()), allocator)) {
        assertTrue(decoded.loadNextBatch());
        var vector = (FixedSizeBinaryVector) decoded.getVectorSchemaRoot().getVector(0);
        for (int i = 0; i < 17; i++) {
          if (i % 2 == 0) {
            assertTrue(vector.isNull(i));
            for (int j = 0; j < width; j++) {
              assertEquals(0, vector.getDataBuffer().getByte((long) i * width + j));
            }
          } else {
            assertArrayEquals(value, vector.get(i));
          }
        }
      }
    }
  }
}
