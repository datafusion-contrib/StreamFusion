package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;

class NativeUdfExactTypesBridgeTest {
  public static class NestedIdentity extends ScalarFunction
      implements NativeUdf.RowResult, NativeUdf.InternalArguments {
    private static final RowType NESTED = RowType.of(new IntType(), new ArrayType(new IntType()));
    private static final RowType RESULT = RowType.of(new ArrayType(new IntType()), NESTED);

    public RowData eval(ArrayData values, RowData record) {
      if (record != null && record.getInt(0) < 0)
        throw new IllegalArgumentException("nested evaluation failed");
      return GenericRowData.of(values, record);
    }

    @Override
    public LogicalType[] argumentTypes() {
      return RESULT.getChildren().toArray(LogicalType[]::new);
    }

    @Override
    public RowType rowResultType() {
      return RESULT;
    }
  }

  @Test
  void nestedSlicesOwnTheirOutputAfterImportedArgumentsClose() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    int id =
        NativeUdf.register(
            new NestedIdentity(),
            NestedIdentity.class.getMethod("eval", ArrayData.class, RowData.class),
            new int[] {NativeUdf.TYPE_INTERNAL, NativeUdf.TYPE_INTERNAL},
            NativeUdf.TYPE_ROW);
    try (ArrowArray output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try (VectorSchemaRoot input = nestedInput(false);
          VectorSchemaRoot slice = input.slice(1, 3)) {
        invoke(id, slice, output, schema);
      }
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, schema, NativeAllocator.DICTIONARIES)) {
        var reader =
            tech.streamfusion.arrow.ArrowConversion.createArrowReader(
                result, RowType.of(NestedIdentity.RESULT));
        RowData first = reader.read(0).getRow(0, 2);
        assertEquals(3, first.getArray(0).size());
        assertEquals(1, first.getArray(0).getInt(0));
        assertTrue(first.getArray(0).isNullAt(1));
        assertEquals(3, first.getArray(0).getInt(2));
        assertEquals(7, first.getRow(1, 2).getInt(0));
        assertEquals(8, first.getRow(1, 2).getArray(1).getInt(0));
        assertTrue(first.getRow(1, 2).getArray(1).isNullAt(1));
        RowData second = reader.read(1).getRow(0, 2);
        assertEquals(0, second.getArray(0).size());
        assertTrue(second.isNullAt(1));
        RowData third = reader.read(2).getRow(0, 2);
        assertTrue(third.isNullAt(0));
        assertEquals(9, third.getRow(1, 2).getInt(0));
        assertEquals(0, third.getRow(1, 2).getArray(1).size());
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void failedNestedEvaluationReleasesInputViewsAndPartialOutput() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    int id =
        NativeUdf.register(
            new NestedIdentity(),
            NestedIdentity.class.getMethod("eval", ArrayData.class, RowData.class),
            new int[] {NativeUdf.TYPE_INTERNAL, NativeUdf.TYPE_INTERNAL},
            NativeUdf.TYPE_ROW);
    try (ArrowArray output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        VectorSchemaRoot input = nestedInput(true)) {
      var error =
          assertThrows(IllegalArgumentException.class, () -> invoke(id, input, output, schema));
      assertEquals("nested evaluation failed", error.getMessage());
    } finally {
      NativeUdf.unregister(id);
      NativeUdf.propagateUpcallFailure(new IllegalStateException("native upcall failed"));
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static VectorSchemaRoot nestedInput(boolean fail) {
    var root =
        VectorSchemaRoot.create(
            tech.streamfusion.arrow.ArrowConversion.toArrowSchema(NestedIdentity.RESULT),
            NativeAllocator.SHARED);
    root.allocateNew();
    var writer =
        tech.streamfusion.arrow.ArrowConversion.createRowDataArrowWriter(
            root, NestedIdentity.RESULT);
    writer.write(GenericRowData.of(new GenericArrayData(new int[] {99}), null));
    writer.write(
        GenericRowData.of(
            new GenericArrayData(new Integer[] {1, null, 3}),
            GenericRowData.of(7, new GenericArrayData(new Integer[] {8, null}))));
    writer.write(GenericRowData.of(new GenericArrayData(new int[0]), null));
    writer.write(
        GenericRowData.of(
            null, GenericRowData.of(fail ? -1 : 9, new GenericArrayData(new int[0]))));
    writer.finish();
    return root;
  }

  public static class DecimalIdentity extends ScalarFunction {
    public BigDecimal eval(BigDecimal value) {
      return value;
    }
  }

  public static class BinaryIdentity extends ScalarFunction {
    public byte[] eval(byte[] value) {
      return value;
    }
  }

  public static class SharedBinaryRow extends ScalarFunction implements NativeUdf.RowResult {
    private final byte[] buffer = new byte[1];
    private final GenericRowData row = new GenericRowData(2);

    public RowData eval(Integer value) {
      if (value == 0) return null;
      if (value == -1) throw new IllegalArgumentException("row evaluation failed");
      buffer[0] = value.byteValue();
      row.setField(0, buffer);
      buffer[0]++;
      row.setField(1, buffer);
      return row;
    }

    @Override
    public RowType rowResultType() {
      return RowType.of(new VarBinaryType(), new VarBinaryType());
    }
  }

  @Test
  void completeRowsRetainAliasesAndSurviveSlicedInputRelease() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var function = new SharedBinaryRow();
    int id =
        NativeUdf.register(
            function,
            SharedBinaryRow.class.getMethod("eval", Integer.class),
            new int[] {NativeUdf.TYPE_INT},
            NativeUdf.TYPE_ROW);
    try (ArrowArray output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try (IntVector values = new IntVector("id", NativeAllocator.SHARED);
          VectorSchemaRoot input = VectorSchemaRoot.of(values)) {
        values.allocateNew();
        for (int i = 0; i < 5; i++) values.setSafe(i, new int[] {99, 1, 0, 3, 4}[i]);
        input.setRowCount(5);
        try (VectorSchemaRoot slice = input.slice(1, 4)) {
          invoke(id, slice, output, schema);
        }
      }
      function.eval(100);
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, schema, NativeAllocator.DICTIONARIES)) {
        StructVector rows = (StructVector) result.getVector(0);
        assertEquals(4, result.getRowCount());
        assertTrue(rows.isNull(1));
        for (var child : rows.getChildrenFromFields()) {
          var bytes = (VarBinaryVector) child;
          assertArrayEquals(new byte[] {2}, bytes.get(0));
          assertArrayEquals(new byte[] {4}, bytes.get(2));
          assertArrayEquals(new byte[] {5}, bytes.get(3));
        }
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void failedRowEvaluationReleasesPartiallyWrittenOutput() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    int id =
        NativeUdf.register(
            new SharedBinaryRow(),
            SharedBinaryRow.class.getMethod("eval", Integer.class),
            new int[] {NativeUdf.TYPE_INT},
            NativeUdf.TYPE_ROW);
    try (ArrowArray output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        IntVector values = new IntVector("id", NativeAllocator.SHARED);
        VectorSchemaRoot input = VectorSchemaRoot.of(values)) {
      values.allocateNew();
      values.setSafe(0, 1);
      values.setSafe(1, -1);
      input.setRowCount(2);
      var failure =
          assertThrows(IllegalArgumentException.class, () -> invoke(id, input, output, schema));
      assertEquals("row evaluation failed", failure.getMessage());
    } finally {
      NativeUdf.unregister(id);
      NativeUdf.propagateUpcallFailure(new IllegalStateException("native upcall failed"));
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void decimalSliceIsNormalizedAndSurvivesInputRelease() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    int id =
        NativeUdf.register(
            new DecimalIdentity(),
            DecimalIdentity.class.getMethod("eval", BigDecimal.class),
            new int[] {NativeUdf.decimalType(8, 3)},
            NativeUdf.decimalType(5, 2));
    try (ArrowArray output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try (DecimalVector values = new DecimalVector("d", NativeAllocator.SHARED, 8, 3);
          VectorSchemaRoot input = VectorSchemaRoot.of(values)) {
        values.allocateNew();
        String[] numbers = {"777.000", "1.235", "-1.235", "999.995", "1.200", null};
        for (int i = 0; i < numbers.length; i++) {
          if (numbers[i] == null) values.setNull(i);
          else values.setSafe(i, new BigDecimal(numbers[i]));
        }
        input.setRowCount(numbers.length);
        try (VectorSchemaRoot slice = input.slice(1, 5)) {
          invoke(id, slice, output, schema);
        }
      }
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, schema, NativeAllocator.DICTIONARIES)) {
        DecimalVector decimals = (DecimalVector) result.getVector(0);
        assertEquals(5, result.getRowCount());
        assertEquals(5, decimals.getPrecision());
        assertEquals(2, decimals.getScale());
        assertEquals(new BigDecimal("1.24"), decimals.getObject(0));
        assertEquals(new BigDecimal("-1.24"), decimals.getObject(1));
        assertTrue(decimals.isNull(2));
        assertEquals(new BigDecimal("1.20"), decimals.getObject(3));
        assertTrue(decimals.isNull(4));
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void binarySlicePreservesNullAndEmptyBytesAfterInputRelease() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    int id =
        NativeUdf.register(
            new BinaryIdentity(),
            BinaryIdentity.class.getMethod("eval", byte[].class),
            new int[] {NativeUdf.TYPE_BINARY},
            NativeUdf.TYPE_BINARY);
    try (ArrowArray output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try (VarBinaryVector values = new VarBinaryVector("b", NativeAllocator.SHARED);
          VectorSchemaRoot input = VectorSchemaRoot.of(values)) {
        values.allocateNew();
        values.setSafe(0, new byte[] {99});
        values.setSafe(1, new byte[] {0, (byte) 0xff, (byte) 0x80, 0});
        values.setSafe(2, new byte[0]);
        values.setNull(3);
        input.setRowCount(4);
        try (VectorSchemaRoot slice = input.slice(1, 3)) {
          invoke(id, slice, output, schema);
        }
      }
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, schema, NativeAllocator.DICTIONARIES)) {
        VarBinaryVector bytes = (VarBinaryVector) result.getVector(0);
        assertEquals(3, result.getRowCount());
        assertArrayEquals(new byte[] {0, (byte) 0xff, (byte) 0x80, 0}, bytes.get(0));
        assertArrayEquals(new byte[0], bytes.get(1));
        assertTrue(bytes.isNull(2));
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static void invoke(
      int id, VectorSchemaRoot input, ArrowArray output, ArrowSchema schema) {
    try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema inputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, input, NativeAllocator.DICTIONARIES, array, inputSchema);
      NativeUdf.invokeUdf(
          id,
          array.memoryAddress(),
          inputSchema.memoryAddress(),
          output.memoryAddress(),
          schema.memoryAddress());
    }
  }
}
