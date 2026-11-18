package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ConstraintTestOperators;

class NativeConstraintEnforcerTest {
  @Test
  void validBatchKeepsIdentityTimestampRoutingAndBuffers() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            harness("first", "second")) {
      ArrowBatch batch = batch(allocator, new Long[][] {{1L, 2L}, {3L, 4L}});
      harness.processElement(new StreamRecord<>(batch, 123));
      StreamRecord<?> record = (StreamRecord<?>) harness.getOutput().remove();
      assertSame(batch, record.getValue());
      assertEquals(123, record.getTimestamp());
      assertEquals(17, batch.keyGroup());
      try (VectorSchemaRoot root = batch.root()) {
        assertEquals(2, root.getRowCount());
        assertEquals(3, ((BigIntVector) root.getVector(0)).get(1));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void nullableColumnsAndEmptyBatchesPass() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            harness(null, "second")) {
      for (Long[][] rows : List.of(new Long[][] {{null, 2L}}, new Long[0][])) {
        ArrowBatch batch = batch(allocator, rows);
        harness.processElement(new StreamRecord<>(batch));
        assertSame(batch, ((StreamRecord<?>) harness.getOutput().remove()).getValue());
        batch.root().close();
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  static Stream<Arguments> violations() {
    return Stream.<Long[][]>of(
            new Long[][] {{null, 2L}},
            new Long[][] {{1L, 2L}, {null, null}},
            new Long[][] {{1L, 2L}, {3L, null}, {null, 4L}},
            new Long[][] {{1L, 2L}, {3L, 4L}, {5L, null}})
        .map(rows -> Arguments.of((Object) rows));
  }

  @ParameterizedTest
  @MethodSource("violations")
  void firstOffendingRowColumnAndValidPrefixMatchStockFlink(Long[][] rows) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> nativeHarness =
            harness("first", "second");
        OneInputStreamOperatorTestHarness<RowData, RowData> stock =
            new OneInputStreamOperatorTestHarness<>(
                ConstraintTestOperators.notNullError("first", "second"))) {
      stock.open();
      RuntimeException expected =
          assertThrows(
              ConstraintTestOperators.notNullExceptionType(),
              () -> {
                for (Long[] row : rows) {
                  stock.processElement(new StreamRecord<>(GenericRowData.of((Object[]) row)));
                }
              });
      ArrowBatch batch = batch(allocator, rows);
      RuntimeException actual =
          assertThrows(
              ConstraintTestOperators.notNullExceptionType(),
              () -> nativeHarness.processElement(new StreamRecord<>(batch, 456)));
      assertEquals(expected.getClass(), actual.getClass());
      assertEquals(expected.getMessage(), actual.getMessage());
      int prefixRows = stock.getOutput().size();
      assertEquals(prefixRows == 0 ? 0 : 1, nativeHarness.getOutput().size());
      if (prefixRows != 0) {
        StreamRecord<?> prefix = (StreamRecord<?>) nativeHarness.getOutput().remove();
        assertEquals(456, prefix.getTimestamp());
        assertEquals(17, ((ArrowBatch) prefix.getValue()).keyGroup());
        try (VectorSchemaRoot root = ((ArrowBatch) prefix.getValue()).root()) {
          assertEquals(prefixRows, root.getRowCount());
          for (int row = 0; row < prefixRows; row++) {
            for (int column = 0; column < 2; column++) {
              assertEquals(
                  rows[row][column].longValue(), ((BigIntVector) root.getVector(column)).get(row));
            }
          }
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void validSharedBatchDoesNotSpendOrCopyEitherConsumersShare() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            harness("first", "second")) {
      ArrowBatch batch = batch(allocator, new Long[][] {{1L, 2L}});
      batch.shareAcross(2);
      harness.processElement(new StreamRecord<>(batch));
      assertSame(batch, ((StreamRecord<?>) harness.getOutput().remove()).getValue());
      try (VectorSchemaRoot first = batch.root();
          VectorSchemaRoot second = batch.root()) {
        assertEquals(
            first.getVector(0).getDataBuffer().memoryAddress(),
            second.getVector(0).getDataBuffer().memoryAddress());
        assertEquals(2, ((BigIntVector) second.getVector(1)).get(0));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void validityCheckPreservesRowKindsAndDoesNotInspectNullableListElements() throws Exception {
    var type = RowType.of(DataTypes.ARRAY(DataTypes.INT()).getLogicalType());
    var row = GenericRowData.of(new GenericArrayData(new Integer[] {1, null, 3}));
    row.setRowKind(RowKind.UPDATE_AFTER);
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness("items")) {
      ArrowBatch batch =
          new ArrowBatch(RowDataArrowConverter.write(List.of(row), type, allocator, true));
      harness.processElement(new StreamRecord<>(batch));
      assertSame(batch, ((StreamRecord<?>) harness.getOutput().remove()).getValue());
      try (VectorSchemaRoot root = batch.root()) {
        RowData actual = RowDataArrowConverter.read(root, type).get(0);
        assertEquals(row.getRowKind(), actual.getRowKind());
        assertTrue(actual.getArray(0).isNullAt(1));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void rejectedPrefixKeepsRowKindsOrderingAndEncodeCallback() throws Exception {
    RowType type = RowType.of(DataTypes.BIGINT().getLogicalType());
    GenericRowData first = GenericRowData.of(1L);
    first.setRowKind(RowKind.UPDATE_BEFORE);
    GenericRowData invalid = GenericRowData.of((Object) null);
    invalid.setRowKind(RowKind.UPDATE_AFTER);
    AtomicLong encodeNanos = new AtomicLong();
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness("id")) {
      ArrowBatch batch =
          new ArrowBatch(
              RowDataArrowConverter.write(List.of(first, invalid), type, allocator, true),
              17,
              91,
              encodeNanos::addAndGet,
              10,
              20,
              30,
              new int[] {7, 9},
              new int[] {2, 3});
      assertThrows(
          ConstraintTestOperators.notNullExceptionType(),
          () -> harness.processElement(new StreamRecord<>(batch)));
      ArrowBatch prefix = (ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue();
      assertEquals(17, prefix.keyGroup());
      assertEquals(91, prefix.handleOwner());
      assertEquals(10, prefix.parentEpochHigh());
      assertEquals(20, prefix.parentEpochLow());
      assertEquals(30, prefix.parentSequence());
      assertArrayEquals(new int[] {7}, prefix.rowOrdinals());
      assertArrayEquals(new int[] {2}, prefix.parentKeyGroups());
      prefix.recordEncodeNanos(42);
      assertEquals(42, encodeNanos.get());
      try (VectorSchemaRoot root = prefix.root()) {
        RowData actual = RowDataArrowConverter.read(root, type).get(0);
        assertEquals(RowKind.UPDATE_BEFORE, actual.getRowKind());
        assertEquals(1, actual.getLong(0));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void rejectedPrefixKeepsSourceWatermarkAndScanMetrics() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            harness("first", "second")) {
      for (boolean watermark : new boolean[] {false, true}) {
        VectorSchemaRoot root = batch(allocator, new Long[][] {{1L, 2L}, {3L, null}}).root();
        ArrowBatch batch =
            watermark
                ? new ArrowBatch(root, new NativeSourceWatermarks.Summary(1234, 1200))
                : new ArrowBatch(root, new ArrowBatch.NativeScanMetrics(1, 2, 3, 4));
        assertThrows(
            ConstraintTestOperators.notNullExceptionType(),
            () -> harness.processElement(new StreamRecord<>(batch)));
        ArrowBatch prefix =
            (ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue();
        assertEquals(batch.sourceWatermarkMillis(), prefix.sourceWatermarkMillis());
        assertSame(batch.nativeScanMetrics(), prefix.nativeScanMetrics());
        prefix.root().close();
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }

  @Test
  void constructorSnapshotsConstraintsAndAllNullableColumnsPass() throws Exception {
    String[] fields = {"first", "second"};
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness(fields)) {
      fields[0] = null;
      ArrowBatch invalid = batch(allocator, new Long[][] {{null, 2L}});
      assertThrows(
          ConstraintTestOperators.notNullExceptionType(),
          () -> harness.processElement(new StreamRecord<>(invalid)));
      assertEquals(0, allocator.getAllocatedMemory());
    }
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness(null, null)) {
      ArrowBatch nullable = batch(allocator, new Long[][] {{null, null}});
      harness.processElement(new StreamRecord<>(nullable));
      assertSame(nullable, ((StreamRecord<?>) harness.getOutput().remove()).getValue());
      nullable.root().close();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void rejectedSharedBatchKeepsBothPrefixAndSiblingAliveInEitherConsumerOrder() throws Exception {
    for (boolean siblingFirst : new boolean[] {false, true}) {
      try (RootAllocator allocator = new RootAllocator();
          OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
              harness("first", "second")) {
        ArrowBatch batch = batch(allocator, new Long[][] {{1L, 2L}, {3L, null}});
        batch.shareAcross(2);
        if (siblingFirst) batch.root().close();
        assertThrows(
            ConstraintTestOperators.notNullExceptionType(),
            () -> harness.processElement(new StreamRecord<>(batch)));
        if (!siblingFirst) {
          try (VectorSchemaRoot sibling = batch.root()) {
            assertEquals(2, sibling.getRowCount());
            assertEquals(3, ((BigIntVector) sibling.getVector(0)).get(1));
          }
        }
        ArrowBatch prefix =
            (ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue();
        try (VectorSchemaRoot root = prefix.root()) {
          assertEquals(1, root.getRowCount());
          assertEquals(1, ((BigIntVector) root.getVector(0)).get(0));
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void prefixRetainsFixedVariableAndNestedBuffersAfterAllInputOwnersClose(boolean shared)
      throws Exception {
    RowType type =
        RowType.of(
            DataTypes.BIGINT().getLogicalType(),
            DataTypes.STRING().getLogicalType(),
            DataTypes.ARRAY(DataTypes.STRING()).getLogicalType(),
            DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT())).getLogicalType());
    GenericRowData first =
        GenericRowData.of(
            1L,
            StringData.fromString("first"),
            new GenericArrayData(new Object[] {StringData.fromString("item"), null}),
            GenericRowData.of(10));
    first.setRowKind(RowKind.UPDATE_BEFORE);
    GenericRowData second = GenericRowData.of(2L, null, null, null);
    GenericRowData invalid = GenericRowData.of(null, null, null, null);
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            harness("id", null, null, null)) {
      VectorSchemaRoot original =
          RowDataArrowConverter.write(List.of(first, second, invalid), type, allocator, true);
      ArrowBuf originalData = original.getVector(0).getDataBuffer();
      long address = originalData.memoryAddress();
      ArrowBatch batch = new ArrowBatch(original);
      if (shared) batch.shareAcross(2);
      assertThrows(
          ConstraintTestOperators.notNullExceptionType(),
          () -> harness.processElement(new StreamRecord<>(batch)));
      if (shared) batch.root().close();

      ArrowBatch prefix = (ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue();
      try (VectorSchemaRoot retained = prefix.root()) {
        assertEquals(0, original.getVector(0).getValueCount());
        assertTrue(originalData.getReferenceManager().getRefCount() > 0);
        assertEquals(address, retained.getVector(0).getDataBuffer().memoryAddress());
        List<RowData> rows = RowDataArrowConverter.read(retained, type);
        assertEquals(2, rows.size());
        assertEquals(RowKind.UPDATE_BEFORE, rows.get(0).getRowKind());
        assertEquals(1, rows.get(0).getLong(0));
        assertEquals("first", rows.get(0).getString(1).toString());
        assertEquals("item", rows.get(0).getArray(2).getString(0).toString());
        assertTrue(rows.get(0).getArray(2).isNullAt(1));
        assertEquals(10, rows.get(0).getRow(3, 1).getInt(0));
        assertEquals(2, rows.get(1).getLong(0));
        for (int column = 1; column < 4; column++) assertTrue(rows.get(1).isNullAt(column));
      }
      assertEquals(0, originalData.getReferenceManager().getRefCount());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void downstreamFailureReleasesUnclaimedPrefixAndRejectedInput() throws Exception {
    RuntimeException failure = new IllegalStateException("injected output failure");
    try (RootAllocator allocator = new RootAllocator();
        var harness =
            ConstraintTestOperators.failingOutputHarness(
                new NativeConstraintEnforcer(new String[] {"first", "second"}), failure)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      ArrowBatch batch = batch(allocator, new Long[][] {{1L, 2L}, {3L, null}});
      assertSame(
          failure,
          assertThrows(
              RuntimeException.class, () -> harness.processElement(new StreamRecord<>(batch))));
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void downstreamFailureDoesNotCloseAnAlreadyConsumedPrefixAgain(boolean shared) throws Exception {
    RuntimeException failure =
        new IllegalStateException("injected output failure after consumption");
    try (RootAllocator allocator = new RootAllocator();
        var harness =
            ConstraintTestOperators.failingOutputHarness(
                new NativeConstraintEnforcer(new String[] {"first", "second"}),
                failure,
                prefix -> prefix.root().close())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      ArrowBatch batch = batch(allocator, new Long[][] {{1L, 2L}, {3L, null}});
      if (shared) batch.shareAcross(2);
      assertSame(
          failure,
          assertThrows(
              RuntimeException.class, () -> harness.processElement(new StreamRecord<>(batch))));
      if (shared) {
        try (VectorSchemaRoot sibling = batch.root()) {
          assertEquals(2, sibling.getRowCount());
          assertEquals(3, ((BigIntVector) sibling.getVector(0)).get(1));
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void watermarksAndConstraintBehaviorSurviveStatelessRestore() throws Exception {
    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        harness("first", "second")) {
      harness.processWatermark(new Watermark(100));
      assertEquals(new Watermark(100), harness.getOutput().remove());
      state = harness.snapshot(1, 1);
    }
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeConstraintEnforcer(new String[] {"first", "second"}))) {
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(state);
      harness.open();
      ArrowBatch batch = batch(allocator, new Long[][] {{null, 2L}});
      assertThrows(
          ConstraintTestOperators.notNullExceptionType(),
          () -> harness.processElement(new StreamRecord<>(batch)));
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void failingSharedConsumerDoesNotCloseSiblingBuffers() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            harness("first", "second")) {
      ArrowBatch batch = batch(allocator, new Long[][] {{null, 2L}});
      batch.shareAcross(2);
      assertThrows(
          ConstraintTestOperators.notNullExceptionType(),
          () -> harness.processElement(new StreamRecord<>(batch)));
      assertTrue(harness.getOutput().isEmpty());
      try (VectorSchemaRoot sibling = batch.root()) {
        assertTrue(sibling.getVector(0).isNull(0));
        assertEquals(2, ((BigIntVector) sibling.getVector(1)).get(0));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness(String... fields)
      throws Exception {
    OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(new NativeConstraintEnforcer(fields));
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    return harness;
  }

  private static ArrowBatch batch(RootAllocator allocator, Long[][] rows) {
    BigIntVector first = new BigIntVector("first", allocator);
    BigIntVector second = new BigIntVector("second", allocator);
    for (BigIntVector column : List.of(first, second)) column.allocateNew(rows.length);
    for (int row = 0; row < rows.length; row++) {
      for (int column = 0; column < 2; column++) {
        BigIntVector vector = column == 0 ? first : second;
        if (rows[row][column] == null) vector.setNull(row);
        else vector.setSafe(row, rows[row][column]);
      }
    }
    first.setValueCount(rows.length);
    second.setValueCount(rows.length);
    VectorSchemaRoot root = new VectorSchemaRoot(List.of(first, second));
    root.setRowCount(rows.length);
    return new ArrowBatch(root, 17);
  }
}
