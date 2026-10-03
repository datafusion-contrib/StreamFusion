package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
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

  @Test
  void fixedBinaryRowsCopyInlineAndVariableValuesBeforeInputReuse() {
    for (int width : new int[] {1, 7, 8, 16, 256}) {
      RowType type = RowType.of(new BinaryType(width));
      try (var allocator = new RootAllocator();
           var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
        var writer = ArrowConversion.createRowDataArrowWriter(root, type, 2);
        var row = new BinaryRowData(1);
        var input = new BinaryRowWriter(row);
        byte[] bytes = new byte[width];
        for (int i = 0; i < 100; i++) {
          input.reset();
          Arrays.fill(bytes, (byte) i);
          if (i % 5 == 0) input.setNullAt(0);
          else input.writeBinary(0, bytes);
          input.complete();
          writer.write(row);
          Arrays.fill(bytes, (byte) 0xff);
          for (var segment : row.getSegments()) Arrays.fill(segment.getArray(), (byte) 0xff);
        }
        writer.finish();
        var binary = (FixedSizeBinaryVector) root.getVector(0);
        assertEquals(100, root.getRowCount());
        for (int i = 0; i < 100; i++) {
          if (i % 5 == 0) assertTrue(binary.isNull(i));
          else {
            byte[] expected = new byte[width];
            Arrays.fill(expected, (byte) i);
            assertArrayEquals(expected, binary.get(i));
          }
        }
      }
    }
  }


  @Test
  void fixedBinaryRowsSupportOffsetsSegmentBoundariesAndOffHeapFallback() {
    for (int width : new int[] {7, 16, 256}) {
      byte[] expected = new byte[width];
      Arrays.fill(expected, (byte) 0x80);
      expected[0] = 0;
      var source = new BinaryRowData(1);
      var sourceWriter = new BinaryRowWriter(source);
      sourceWriter.writeBinary(0, expected);
      sourceWriter.complete();
      byte[] serialized = source.getSegments()[0].getArray();
      int size = source.getSizeInBytes();
      int base = 8;
      for (int layout = 0; layout < 3; layout++) {
        MemorySegment[] segments;
        if (layout == 0) {
          byte[] storage = new byte[base + size];
          System.arraycopy(serialized, 0, storage, base, size);
          segments = new MemorySegment[] {MemorySegmentFactory.wrap(storage)};
        } else if (layout == 1) {
          int segmentSize = 32;
          segments = new MemorySegment[(base + size + segmentSize - 1) / segmentSize];
          for (int i = 0; i < segments.length; i++) {
            segments[i] = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
          }
          for (int i = 0; i < size; i++) {
            int offset = base + i;
            segments[offset / segmentSize].put(offset % segmentSize, serialized[i]);
          }
        } else {
          segments = new MemorySegment[] {MemorySegmentFactory.allocateUnpooledOffHeapMemory(base + size)};
          segments[0].put(base, serialized, 0, size);
        }
        try (var allocator = new RootAllocator();
             var root = VectorSchemaRoot.create(
                 ArrowConversion.toArrowSchema(RowType.of(new BinaryType(width))), allocator)) {
          var row = new BinaryRowData(1);
          row.pointTo(segments, base, size);
          assertArrayEquals(expected, row.getBinary(0));
          var writer = ArrowConversion.createRowDataArrowWriter(root, RowType.of(new BinaryType(width)), 1);
          writer.write(row);
          for (var segment : segments) {
            for (int i = 0; i < segment.size(); i++) segment.put(i, (byte) 0xff);
          }
          writer.finish();
          assertArrayEquals(expected, ((FixedSizeBinaryVector) root.getVector(0)).get(0));
        } finally {
          if (layout == 2) segments[0].free();
        }
      }
    }
  }

}
