package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IntArrayWriterOwnershipTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ownsPrimitiveAndNullableElementsAcrossGrowthAndFreshBatches(boolean nullable) {
    var type = RowType.of(new ArrayType(new IntType(nullable)));
    try (var allocator = new RootAllocator();
         var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type, 2);
      for (int row = 0; row < 1027; row++) {
        int size = row % 3 == 0 ? 0 : 67;
        int[] primitive = new int[size];Integer[] boxed = new Integer[size];
        for (int col = 0; col < size; col++) {
          primitive[col] = row * 100 + col;
          boxed[col] = nullable && col % 5 == 0 ? null : primitive[col];
        }
        var array = nullable ? new GenericArrayData(boxed) : new GenericArrayData(primitive);
        writer.write(GenericRowData.of(row % 7 == 0 ? null : array));
        Arrays.fill(primitive, -1);Arrays.fill(boxed, -1);
      }
      writer.finish();
      var reader = ArrowConversion.createArrowReader(root, type);
      for (int row = 0; row < 1027; row++) {
        var value = reader.read(row);
        assertEquals(row % 7 == 0, value.isNullAt(0));
        if (row % 7 == 0) continue;
        var array = value.getArray(0);assertEquals(row % 3 == 0 ? 0 : 67, array.size());
        for (int col = 0; col < array.size(); col++) {
          assertEquals(nullable && col % 5 == 0, array.isNullAt(col));
          if (!array.isNullAt(col)) assertEquals(row * 100 + col, array.getInt(col));
        }
      }
      var child = (IntVector) ((ListVector) root.getVector(0)).getDataVector();
      var data = child.getDataBuffer();var validity = child.getValidityBuffer();
      data.getReferenceManager().retain();validity.getReferenceManager().retain();
      try {
        var next = ArrowConversion.createRowDataArrowWriter(root, type, 2);
        assertNotEquals(data.memoryAddress(), child.getDataBuffer().memoryAddress());
        next.write(GenericRowData.of((Object) null));next.finish();
        assertEquals(101, data.getInt(4));assertNotEquals(0, validity.getByte(0) & 2);
      } finally {validity.close();data.close();}
    }
  }
  @org.junit.jupiter.api.Test
  void preservesNestedArrayOffsetsAndNullsAcrossGrowth() {
    var type = RowType.of(new ArrayType(new ArrayType(new IntType())));
    try (var allocator = new RootAllocator();
         var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type, 1);
      for (int row = 0; row < 517; row++) {
        var elements = new Integer[] {Integer.MIN_VALUE, null, row, Integer.MAX_VALUE};
        var arrays = new Object[] {new GenericArrayData(elements), null, new GenericArrayData(new int[0])};
        writer.write(GenericRowData.of(new GenericArrayData(arrays)));
        Arrays.fill(elements, -1);
        Arrays.fill(arrays, null);
      }
      writer.finish();
      var reader = ArrowConversion.createArrowReader(root, type);
      for (int row = 0; row < 517; row++) {
        var outer = reader.read(row).getArray(0);
        assertEquals(3, outer.size());
        assertTrue(outer.isNullAt(1));
        assertEquals(0, outer.getArray(2).size());
        var inner = outer.getArray(0);
        assertEquals(4, inner.size());
        assertEquals(Integer.MIN_VALUE, inner.getInt(0));
        assertTrue(inner.isNullAt(1));
        assertEquals(row, inner.getInt(2));
        assertEquals(Integer.MAX_VALUE, inner.getInt(3));
      }
    }
  }

}
