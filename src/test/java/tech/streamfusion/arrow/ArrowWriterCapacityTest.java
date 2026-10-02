package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class ArrowWriterCapacityTest {
  @Test
  void fixedCapacityCanGrowAndOwnsReusedInputWhileVariableCapacityStaysDefault() {
    RowType type = RowType.of(new BinaryType(256), new VarCharType());
    try (var allocator = new RootAllocator();
         var sized = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator);
         var defaults = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(sized, type, 2);
      ArrowConversion.createRowDataArrowWriter(defaults, type);
      assertTrue(sized.getVector(0).getValueCapacity() < defaults.getVector(0).getValueCapacity());
      assertEquals(defaults.getVector(1).getValueCapacity(), sized.getVector(1).getValueCapacity());
      byte[] reused = new byte[256];
      for (int i = 0; i < 100; i++) {
        Arrays.fill(reused, (byte) i);
        writer.write(GenericRowData.of(i % 5 == 0 ? null : reused, StringData.fromString("row" + i)));
        Arrays.fill(reused, (byte) 0xff);
      }
      writer.finish();
      assertEquals(100, sized.getRowCount());
      var binary = (FixedSizeBinaryVector) sized.getVector(0);
      var reader = ArrowConversion.createArrowReader(sized, type);
      for (int i = 0; i < 100; i++) {
        if (i % 5 == 0) assertTrue(binary.isNull(i));
        else {
          byte[] expected = new byte[256];
          Arrays.fill(expected, (byte) i);
          assertArrayEquals(expected, binary.get(i));
        }
        assertEquals("row" + i, reader.read(i).getString(1).toString());
      }
    }
  }
}
