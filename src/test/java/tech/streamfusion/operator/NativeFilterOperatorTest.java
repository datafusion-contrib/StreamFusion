package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ByteStringTransport;

class NativeFilterOperatorTest {
  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  void filtersABatchKeepingSurvivors() throws Exception {
    // Predicate v > 20 (column index 1, int literal): CALL gt ( INPUT_REF 1, LIT_INT 20 ).
    NativeFilterOperator operator =
        new NativeFilterOperator(
            new int[] {0, 1}, // identity projection
            new int[] {6, 0, 7},
            new int[] {10, 1, 0},
            new int[] {2, 0, 0},
            new long[] {20},
            new double[] {},
            new String[] {},
            NativeUdf.Binding.EMPTY);
    List<RowData> output = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      VectorSchemaRoot in =
          RowDataArrowConverter.write(List.of(row(1L, 10), row(2L, 30), row(3L, 20)), SCHEMA, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(in)));

      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord) {
          ArrowBatch batch = ((StreamRecord<ArrowBatch>) record).getValue();
          try (VectorSchemaRoot root = batch.root()) {
            output.addAll(RowDataArrowConverter.read(root, SCHEMA));
          }
        }
      }
    }
    assertEquals(1, output.size(), "only v=30 survives v>20");
    assertEquals(30, output.get(0).getInt(1));
  }

  @Test
  void repeatedNullableColumnsRemainReadableAfterInputRelease() throws Exception {
    RowType outputType =
        RowType.of(
            new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType(), new IntType()},
            new String[] {"a", "b", "c", "v"});
    List<RowData> rows =
        filter(
            new int[] {0, 0, 0, 1},
            List.of(row(1, 10), GenericRowData.of(null, 30), row(99, 40)),
            SCHEMA,
            outputType,
            false);

    assertEquals(2, rows.size());
    for (int column = 0; column < 3; column++) {
      assertTrue(rows.get(0).isNullAt(column));
      assertEquals(99, rows.get(1).getLong(column));
    }
    assertEquals(30, rows.get(0).getInt(3));
    assertEquals(40, rows.get(1).getInt(3));
  }

  @Test
  void repeatedNestedColumnsKeepNullsEmptyArraysAndChangelogKinds() throws Exception {
    ArrayType arrayType = new ArrayType(new IntType());
    RowType inputType =
        RowType.of(
            new LogicalType[] {new BigIntType(), new IntType(), arrayType},
            new String[] {"k", "v", "items"});
    RowType outputType =
        RowType.of(
            new LogicalType[] {arrayType, arrayType, new BigIntType()},
            new String[] {"a", "b", "k"});
    List<RowData> rows =
        filter(
            new int[] {2, 2, 0},
            List.of(
                GenericRowData.of(1L, 10, new GenericArrayData(new int[] {0})),
                GenericRowData.ofKind(
                    RowKind.UPDATE_BEFORE, 7L, 30, new GenericArrayData(new Integer[] {1, null, 3})),
                GenericRowData.ofKind(
                    RowKind.UPDATE_AFTER, 7L, 40, new GenericArrayData(new int[] {})),
                GenericRowData.ofKind(RowKind.DELETE, 8L, 50, null)),
            inputType,
            outputType,
            true);

    assertEquals(3, rows.size());
    assertEquals(RowKind.UPDATE_BEFORE, rows.get(0).getRowKind());
    assertEquals(RowKind.UPDATE_AFTER, rows.get(1).getRowKind());
    assertEquals(RowKind.DELETE, rows.get(2).getRowKind());
    for (int column = 0; column < 2; column++) {
      var array = rows.get(0).getArray(column);
      assertEquals(3, array.size());
      assertEquals(1, array.getInt(0));
      assertTrue(array.isNullAt(1));
      assertEquals(3, array.getInt(2));
      assertEquals(0, rows.get(1).getArray(column).size());
      assertTrue(rows.get(2).isNullAt(column));
    }
    assertEquals(7, rows.get(0).getLong(2));
    assertEquals(7, rows.get(1).getLong(2));
    assertEquals(8, rows.get(2).getLong(2));
  }

  @Test
  void repeatedByteStringProjectionPreservesOriginalBytesAndJavaCache() throws Exception {
    RowType inputType =
        RowType.of(
            new LogicalType[] {new VarCharType(), new IntType()}, new String[] {"s", "v"});
    RowType outputType =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(), new VarCharType()},
            new String[] {"v", "first", "second"});
    var schema = ByteStringTransport.inputSchema(ArrowConversion.toArrowSchema(inputType),
        new int[] {0});
    var bytes = new byte[] {(byte) 0xff};
    var cached = BinaryStringData.fromBytes(bytes);
    String decoded = cached.toString();
    var cold = BinaryStringData.fromBytes(bytes);
    var transpose =
        new RowDataToArrowOperator(inputType, 3, true, null, true, schema.serializeAsMessage());
    try (var entry = new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(transpose);
        var filter = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            numericFilter(new int[] {1, 0, 0}, new String[] {"v", "first", "second"}),
            new ArrowBatchSerializer())) {
      entry.setup(new ArrowBatchSerializer());
      filter.setup(new ArrowBatchSerializer());
      entry.open();
      filter.open();

      entry.processElement(new StreamRecord<>(GenericRowData.of(cold, 10)));
      entry.processElement(new StreamRecord<>(GenericRowData.of(cached, 30)));
      entry.processElement(new StreamRecord<>(GenericRowData.of(cold, 40)));
      for (var record : entry.extractOutputStreamRecords()) {
        filter.processElement(new StreamRecord<>(record.getValue()));
      }
      var batches = filter.extractOutputStreamRecords();
      assertEquals(1, batches.size());
      try (var root = batches.get(0).getValue().root()) {
        assertEquals(2, root.getRowCount());
        assertEquals("first", root.getVector(1).getName());
        assertEquals("second", root.getVector(2).getName());
        assertTrue(ByteStringTransport.isString(root.getSchema().getFields().get(1)));
        assertTrue(ByteStringTransport.isString(root.getSchema().getFields().get(2)));
        var reader = ArrowConversion.createArrowReader(root, outputType);
        for (int column = 1; column <= 2; column++) {
          var warmValue = (BinaryStringData) reader.read(0).getString(column);
          assertArrayEquals(bytes, warmValue.toBytes());
          assertEquals(decoded, warmValue.getJavaObject());
          var coldValue = (BinaryStringData) reader.read(1).getString(column);
          assertArrayEquals(bytes, coldValue.toBytes());
          assertNull(coldValue.getJavaObject());
        }
      }
    }
  }

  private static NativeFilterOperator numericFilter(int[] projection, String[] outputNames) {
    return new NativeFilterOperator(
        projection,
        new int[] {6, 0, 7},
        new int[] {10, 1, 0},
        new int[] {2, 0, 0},
        new long[] {20},
        new double[] {},
        new String[] {},
        NativeUdf.Binding.EMPTY,
        outputNames);
  }

  private static List<RowData> filter(
      int[] projection,
      List<RowData> rows,
      RowType inputType,
      RowType outputType,
      boolean withRowKind)
      throws Exception {
    NativeFilterOperator operator = numericFilter(projection, null);
    List<RowData> result = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(rows, inputType, allocator, withRowKind))));
      for (Object emitted : harness.getOutput()) {
        if (emitted instanceof StreamRecord<?>) {
          StreamRecord<?> record = ((StreamRecord<?>) emitted);

          try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
            result.addAll(RowDataArrowConverter.read(root, outputType));
          }
        }
      }
      assertEquals(0, allocator.getAllocatedMemory(), "filter must release its input buffers");
    }
    return result;
  }
}
