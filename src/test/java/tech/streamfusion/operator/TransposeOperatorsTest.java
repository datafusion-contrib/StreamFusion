package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransposeOperatorsTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @SuppressWarnings("unchecked")
  private static <T> List<T> values(OneInputStreamOperatorTestHarness<?, T> harness) {
    List<T> out = new ArrayList<>();
    for (Object record : harness.getOutput()) {
      if (record instanceof StreamRecord) {
        out.add((T) ((StreamRecord<T>) record).getValue());
      }
    }
    return out;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rowsTransposeToBatchesAndBack(boolean objectReuse) throws Exception {
    List<RowData> input = List.of(row(1L, 10), row(2L, 20), row(3L, 30), row(4L, 40), row(5L, 50));

    // Declared first so it closes last: its allocator outlives the batches the second harness frees.
    try (OneInputStreamOperatorTestHarness<RowData, ArrowBatch> toArrow =
            new OneInputStreamOperatorTestHarness<>(new RowDataToArrowOperator(SCHEMA, 2, false, null));
        OneInputStreamOperatorTestHarness<ArrowBatch, RowData> toRows =
            new OneInputStreamOperatorTestHarness<>(
                new ArrowToRowDataOperator(SCHEMA), new ArrowBatchSerializer())) {
      // The harness copies records by serializer; tell it to use the batch serializer (identity copy)
      // for the ArrowBatch edges rather than falling back to Kryo, which cannot copy an Arrow batch.
      toArrow.setup(new ArrowBatchSerializer());
      // Exercise the chained-runtime contract: with object reuse enabled the harness retains the
      // exact RowData objects emitted by the boundary, so the operator itself must own each row.
      if (objectReuse) toRows.getExecutionConfig().enableObjectReuse();
      toRows.setup(new RowDataSerializer(SCHEMA));
      toArrow.open();
      toRows.open();
      for (RowData r : input) {
        toArrow.processElement(new StreamRecord<>(r));
      }
      toArrow.endInput();

      // Batch size 2 over 5 rows → batches of 2, 2, 1.
      List<ArrowBatch> batches = values(toArrow);
      assertEquals(3, batches.size());
      assertEquals(5, batches.stream().mapToInt(ArrowBatch::rowCount).sum());

      for (ArrowBatch batch : batches) {
        toRows.processElement(new StreamRecord<>(batch));
      }

      List<RowData> output = values(toRows);
      assertEquals(input.size(), output.size());
      for (int i = 0; i < input.size(); i++) {
        assertEquals(input.get(i).getLong(0), output.get(i).getLong(0), "k row " + i);
        assertEquals(input.get(i).getInt(1), output.get(i).getInt(1), "v row " + i);
      }
    }
  }

  @Test
  void rowToArrowOwnsBufferedRowsFromReusingUpstream() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(new RowDataToArrowOperator(SCHEMA, 2, false, null))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      GenericRowData reused = new GenericRowData(2);
      reused.setField(0, 1L);
      reused.setField(1, 10);
      harness.processElement(new StreamRecord<>(reused));
      reused.setField(0, 2L);
      reused.setField(1, 20);
      harness.processElement(new StreamRecord<>(reused));

      List<ArrowBatch> batches = values(harness);
      assertEquals(1, batches.size());
      try (var root = batches.get(0).root()) {
        List<RowData> rows = RowDataArrowConverter.read(root, SCHEMA);
        assertEquals(1L, rows.get(0).getLong(0));
        assertEquals(10, rows.get(0).getInt(1));
        assertEquals(2L, rows.get(1).getLong(0));
        assertEquals(20, rows.get(1).getInt(1));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exitRowsKeepNestedValuesAfterArrowBatchCloses(boolean objectReuse) throws Exception {
    var nested = RowType.of(new org.apache.flink.table.types.logical.VarCharType());
    var schema = RowType.of(new org.apache.flink.table.types.logical.VarBinaryType(), nested);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, RowData>(
        new ArrowToRowDataOperator(schema), new ArrowBatchSerializer())) {
      if (objectReuse) harness.getExecutionConfig().enableObjectReuse();
      harness.setup(new RowDataSerializer(schema));
      harness.open();
      for (int i = 0; i < 3; i++) {
        var input = GenericRowData.of(new byte[] {(byte) i},
            GenericRowData.of(org.apache.flink.table.data.StringData.fromString("row-" + i)));
        var root = RowDataArrowConverter.write(List.of(input), schema, NativeAllocator.SHARED);
        harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      }
      List<RowData> rows = values(harness);
      assertEquals(3, rows.size());
      for (int i = 0; i < rows.size(); i++) {
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] {(byte) i}, rows.get(i).getBinary(0));
        assertEquals("row-" + i, rows.get(i).getRow(1, 1).getString(0).toString());
      }
    }
  }

  @Test
  void checkpointBarrierDrainsPartialRowToArrowBatch() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(new RowDataToArrowOperator(SCHEMA, 3, false, null))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(row(1L, 10)));
      harness.processElement(new StreamRecord<>(row(2L, 20)));
      assertEquals(List.of(), values(harness));

      harness.prepareSnapshotPreBarrier(1L);
      List<ArrowBatch> batches = values(harness);
      assertEquals(1, batches.size());
      try (var root = batches.get(0).root()) {
        assertEquals(2, root.getRowCount());
      }
    }
  }

  @Test
  void closingPartialBatchReleasesOwnedBuffersWithoutEmitting() throws Exception {
    long before;
    try (var harness = new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(
        new RowDataToArrowOperator(SCHEMA, 3, true, null))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      before = NativeAllocator.SHARED.getAllocatedMemory();
      harness.processElement(new StreamRecord<>(row(1L, 10)));
      assertTrue(NativeAllocator.SHARED.getAllocatedMemory() > before);
      assertTrue(harness.getOutput().isEmpty());
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void failedWriteReleasesPartiallyWrittenBatchImmediately() throws Exception {
    try (var harness = new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(
        new RowDataToArrowOperator(SCHEMA, 3, true, null))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      long before = NativeAllocator.SHARED.getAllocatedMemory();
      harness.processElement(new StreamRecord<>(row(1L, 10)));
      var invalid = GenericRowData.of(2L, "not an integer");
      assertThrows(ClassCastException.class,
          () -> harness.processElement(new StreamRecord<>(invalid)));
      assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  @Test
  void processingTimeDrainsPartialRowToArrowBatchWithoutWatermarks() throws Exception {
    String property = "streamfusion.transpose.flushLatencyMs";
    String previous = System.getProperty(property);
    System.setProperty(property, "10");
    try (OneInputStreamOperatorTestHarness<RowData, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(new RowDataToArrowOperator(SCHEMA, 3, false, null))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(100L);
      harness.processElement(new StreamRecord<>(row(1L, 10)));
      assertEquals(List.of(), values(harness));

      harness.setProcessingTime(109L);
      assertEquals(List.of(), values(harness));
      harness.setProcessingTime(110L);
      List<ArrowBatch> batches = values(harness);
      assertEquals(1, batches.size());
      try (var root = batches.get(0).root()) {
        assertEquals(1, root.getRowCount());
      }
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }
}
