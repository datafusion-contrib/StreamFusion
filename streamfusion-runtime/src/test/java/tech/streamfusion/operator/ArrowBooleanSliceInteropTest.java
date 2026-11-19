package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;

class ArrowBooleanSliceInteropTest {
  private static final RowType TYPE =
      RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new IntType(),
            new IntType(),
            new BooleanType(),
            RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {new BooleanType()},
                new String[] {"bit"}),
            new ArrayType(new BooleanType())
          },
          new String[] {"id", "partition", "flag", "nested", "flags"});

  @ParameterizedTest
  @ValueSource(ints = {1, 7, 8, 9, 63, 64, 65, 127})
  void partitionSlicesPreserveBooleanValuesNullsAndNestedChildren(int offset) {
    for (boolean reverse : new boolean[] {false, true}) {
      try (var allocator = new RootAllocator()) {
        List<RowData> rows = new ArrayList<>();
        for (int i = 0; i < offset + 17; i++) {
          Boolean bit = i % 11 == 3 ? null : ((i % 2 == 0) ^ reverse);
          rows.add(
              GenericRowData.of(
                  i,
                  i < offset ? 0 : 1,
                  bit,
                  i % 5 == 4 ? null : GenericRowData.of(bit),
                  i % 5 == 4 ? null : new GenericArrayData(new Boolean[] {bit, null, !reverse})));
        }
        long handle;
        try (var root = RowDataArrowConverter.write(rows, TYPE, allocator);
            var array = ArrowArray.allocateNew(allocator);
            var schema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
          handle =
              Native.splitByPartitionColumns(
                  array.memoryAddress(), schema.memoryAddress(), new int[] {1});
        }
        int seen = 0;
        try {
          for (int length : new int[] {offset, 17}) {
            try (var array = ArrowArray.allocateNew(allocator);
                var schema = ArrowSchema.allocateNew(allocator)) {
              assertTrue(
                  Native.nextPartitionSlice(handle, array.memoryAddress(), schema.memoryAddress()));
              try (VectorSchemaRoot root =
                  Data.importVectorSchemaRoot(
                      allocator, array, schema, NativeAllocator.DICTIONARIES)) {
                assertEquals(length, root.getRowCount());
                for (int i = 0; i < length; i++, seen++) {
                  Boolean expected = seen % 11 == 3 ? null : ((seen % 2 == 0) ^ reverse);
                  assertEquals(seen, root.getVector("id").getObject(i));
                  assertEquals(expected, root.getVector("flag").getObject(i), "row " + seen);
                  var nested = (StructVector) root.getVector("nested");
                  assertEquals(seen % 5 == 4, nested.isNull(i));
                  if (!nested.isNull(i))
                    assertEquals(expected, nested.getChild("bit").getObject(i));
                  assertEquals(
                      seen % 5 == 4 ? null : Arrays.asList(expected, null, !reverse),
                      root.getVector("flags").getObject(i));
                }
              }
            }
          }
        } finally {
          Native.closePartitionSplit(handle);
        }
        assertEquals(rows.size(), seen);
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }
}
