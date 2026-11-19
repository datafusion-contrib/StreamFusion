package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeBatchAggregateOperatorTest {
  private long previous;
  private static final RowType INPUT = RowType.of(new BigIntType(), new BooleanType());
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());

  @BeforeEach
  void configure() {
    previous = TaskOffHeapMemory.capacityBytes();
    TaskOffHeapMemory.resetForTests(2L << 20);
  }

  @AfterEach
  void clean() throws Exception {
    for (int i = 0; i < 100 && TaskOffHeapMemory.reservedBytes() != 0; i++) Thread.sleep(10);
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.resetForTests(previous == 0 ? -1 : previous);
  }

  @Test
  void emptyFinalAggregateProducesOneDefaultRowAndLocalProducesNone() throws Exception {
    for (boolean finalOutput : List.of(false, true)) {
      try (var harness =
          harness(
              INPUT,
              finalOutput
                  ? OUTPUT
                  : RowType.of(
                      new BigIntType(),
                      new BigIntType(),
                      new BigIntType(),
                      new BigIntType(),
                      new BigIntType(),
                      new BigIntType()),
              new int[] {0, 1, 2, 3, 4},
              new int[] {0, 0, 0, 0, 0},
              new int[] {-1, -1, -1, -1, -1},
              false,
              finalOutput)) {
        harness.endInput();
        assertEquals(finalOutput ? List.of("null:0:null:null:null") : List.of(), drain(harness));
      }
    }
  }

  @Test
  void multipleInputsApplyFilterAndIntegerAverageBeforeFinalOutput() throws Exception {
    try (var harness =
        harness(
            INPUT,
            OUTPUT,
            new int[] {0, 1, 2, 3, 4},
            new int[] {0, 0, 0, 0, 0},
            new int[] {1, 1, 1, 1, 1},
            false,
            true)) {
      push(harness, List.of(GenericRowData.of(10L, true), GenericRowData.of(20L, false)));
      assertTrue(harness.getOutput().isEmpty());
      push(harness, List.of(GenericRowData.of(-5L, true), GenericRowData.of(null, true)));
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
      harness.endInput();
      assertEquals(List.of("5:2:-5:10:2"), drain(harness));
    }
  }

  @Test
  void rejectedFactoryReleasesBothSchemasAndTheOfferedBatch() throws Exception {
    try (var harness =
        harness(INPUT, OUTPUT, new int[] {99}, new int[] {0}, new int[] {-1}, false, true)) {
      assertThrows(
          tech.streamfusion.NativeException.class,
          () -> push(harness, List.of(GenericRowData.of(7L, true))));
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
    }
  }

  @Test
  void cancellationBeforeEofReleasesScalarStateAndBorrowedInput() throws Exception {
    try (var harness =
        harness(
            INPUT,
            OUTPUT,
            new int[] {0, 1, 2, 3, 4},
            new int[] {0, 0, 0, 0, 0},
            new int[] {-1, -1, -1, -1, -1},
            false,
            true)) {
      push(harness, List.of(GenericRowData.of(7L, true)));
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  @Test
  void finalResultCanOutliveTheAggregateWithItsDifferentSchema() throws Exception {
    org.apache.arrow.vector.VectorSchemaRoot result = null;
    try {
      try (var harness =
          harness(
              INPUT,
              OUTPUT,
              new int[] {0, 1, 2, 3, 4},
              new int[] {0, 0, 0, 0, 0},
              new int[] {-1, -1, -1, -1, -1},
              false,
              true)) {
        push(harness, List.of(GenericRowData.of(7L, true)));
        harness.endInput();
        var record = (StreamRecord<?>) harness.getOutput().poll();
        result = ((ArrowBatch) record.getValue()).root();
      }
      assertEquals(5, result.getFieldVectors().size());
      assertEquals(7L, result.getVector(0).getObject(0));
      assertEquals(1L, result.getVector(1).getObject(0));
      assertTrue(TaskOffHeapMemory.reservedBytes() > 0);
    } finally {
      if (result != null) result.close();
    }
  }

  @Test
  void expressionWorkspaceDenialReleasesTheBorrowedInput() throws Exception {
    try (var harness =
        harness(
            INPUT,
            OUTPUT,
            new int[] {0, 1, 2, 3, 4},
            new int[] {0, 0, 0, 0, 0},
            new int[] {-1, -1, -1, -1, -1},
            false,
            true)) {
      List<RowData> rows = new ArrayList<>();
      for (int index = 0; index < 32768; index++) rows.add(GenericRowData.of((long) index, true));
      assertThrows(tech.streamfusion.NativeMemoryLimitException.class, () -> push(harness, rows));
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void countUsesValidityForStringsBooleansDecimalsAndTemporalScalars(boolean filtered)
      throws Exception {
    RowType input =
        RowType.of(
            new VarCharType(),
            new BooleanType(),
            new DecimalType(10, 2),
            new DateType(),
            new TimeType(),
            new TimestampType(3));
    LogicalType[] counts = new LogicalType[7];
    Arrays.fill(counts, new BigIntType(false));
    int[] filters = new int[7];
    Arrays.fill(filters, filtered ? 1 : -1);
    try (var harness =
        harness(
            input,
            RowType.of(counts),
            new int[] {1, 1, 1, 1, 1, 1, 1},
            new int[] {0, 1, 2, 3, 4, 5, -1},
            filters,
            false,
            true)) {
      var rows =
          List.<RowData>of(
              GenericRowData.of(
                  StringData.fromString("x"),
                  true,
                  DecimalData.fromBigDecimal(new BigDecimal("1.25"), 10, 2),
                  10,
                  43_200_000,
                  TimestampData.fromEpochMillis(1000)),
              GenericRowData.of(
                  StringData.fromString(""),
                  false,
                  DecimalData.fromBigDecimal(BigDecimal.ZERO, 10, 2),
                  0,
                  0,
                  TimestampData.fromEpochMillis(0)),
              new GenericRowData(6));
      var root = RowDataArrowConverter.write(rows, input, NativeAllocator.SHARED, false);
      harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
      harness.endInput();
      assertEquals(List.of(filtered ? "1:1:1:1:1:1:1" : "2:2:2:2:2:2:3"), drain(harness));
    }
  }

  @Test
  void extremaNormalizeForeignTimeUnitsBeforeComparing() throws Exception {
    RowType input = RowType.of(new TimeType(0));
    RowType resultType = RowType.of(new TimeType(0), new TimeType(0));
    try (var harness =
        harness(
            input,
            resultType,
            new int[] {2, 3},
            new int[] {0, 0},
            new int[] {-1, -1},
            false,
            true)) {
      var values = new org.apache.arrow.vector.TimeMicroVector("f0", NativeAllocator.SHARED);
      values.allocateNew(4);
      values.setNull(0);
      values.setSafe(1, -1_234_999L);
      values.setSafe(2, 1_234_999L);
      values.setSafe(3, Long.MAX_VALUE);
      values.setValueCount(4);
      var root = org.apache.arrow.vector.VectorSchemaRoot.of(values);
      root.setRowCount(4);
      int low = Math.min(-1234, (int) (Long.MAX_VALUE / 1000));
      int high = Math.max(1234, (int) (Long.MAX_VALUE / 1000));
      harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
      harness.endInput();
      var record = (StreamRecord<?>) harness.getOutput().poll();
      try (var result = ((ArrowBatch) record.getValue()).root()) {
        assertEquals(low, ((org.apache.arrow.vector.TimeMilliVector) result.getVector(0)).get(0));
        assertEquals(high, ((org.apache.arrow.vector.TimeMilliVector) result.getVector(1)).get(0));
      }
    }
  }

  @Test
  void countStarCarriesZeroColumnBatchRowCounts() throws Exception {
    var empty = RowType.of(new LogicalType[0]);
    var count = RowType.of(new BigIntType(false));
    try (var harness =
        harness(empty, count, new int[] {1}, new int[] {-1}, new int[] {-1}, false, true)) {
      var root =
          RowDataArrowConverter.write(
              List.of(GenericRowData.of(), GenericRowData.of(), GenericRowData.of()),
              empty,
              NativeAllocator.SHARED,
              false);
      harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      harness.endInput();
      assertEquals(List.of("3"), drain(harness));
    }
  }

  @Test
  void groupedHashStateSpillsAndReplaysWithinTheExistingAuthority() throws Exception {
    var input = RowType.of(new BigIntType(), new BigIntType());
    var outputType = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var operator =
        new NativeBatchAggregateOperator(
            input,
            outputType,
            new int[] {0},
            new int[] {0, 1},
            new int[] {1, 1},
            new int[] {-1, -1},
            false,
            true,
            128);
    var seen = new java.util.BitSet();
    try (var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      var outputField =
          org.apache.flink.streaming.api.operators.AbstractStreamOperator.class.getDeclaredField(
              "output");
      outputField.setAccessible(true);
      outputField.set(
          operator,
          new tech.streamfusion.compat.FlinkOutput<ArrowBatch>(null) {
            @Override
            public void collect(StreamRecord<ArrowBatch> record) {
              try (var root = record.getValue().root()) {
                assertTrue(root.getRowCount() <= 128);
                var key = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
                var sum = (org.apache.arrow.vector.BigIntVector) root.getVector(1);
                var count = (org.apache.arrow.vector.BigIntVector) root.getVector(2);
                for (int row = 0; row < root.getRowCount(); row++) {
                  int value = (int) key.get(row);
                  assertFalse(seen.get(value));
                  seen.set(value);
                  assertEquals(4L * value, sum.get(row));
                  assertEquals(4L, count.get(row));
                }
              }
            }
          });
      for (int start = 0; start < 262144; start += 128) {
        List<RowData> rows = new ArrayList<>();
        for (int row = start; row < start + 128; row++)
          rows.add(GenericRowData.of((long) (row % 65536), (long) (row % 65536)));
        var root = RowDataArrowConverter.write(rows, input, NativeAllocator.SHARED, false);
        harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
        assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
        assertTrue(TaskOffHeapMemory.reservedBytes() <= 2L << 20);
      }
      var statsField = NativeBoundedBatchOperator.class.getDeclaredField("executionStats");
      statsField.setAccessible(true);
      long[] stats = (long[]) statsField.get(operator);
      assertTrue(stats[0] > 0 && stats[1] > 0, "Native grouped spill must occur before EOF");
      harness.endInput();
      assertEquals(65536, seen.cardinality());
      for (var directory : harness.getEnvironment().getIOManager().getSpillingDirectories()) {
        try (var files = java.nio.file.Files.walk(directory.toPath())) {
          assertEquals(0, files.filter(java.nio.file.Files::isRegularFile).count());
        }
      }
    }
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness(
      RowType input,
      RowType output,
      int[] kinds,
      int[] columns,
      int[] filters,
      boolean merge,
      boolean finalOutput)
      throws Exception {
    var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new NativeBatchAggregateOperator(
                input, output, kinds, columns, filters, merge, finalOutput, 2),
            new ArrowBatchSerializer());
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    return harness;
  }

  private static void push(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness, List<RowData> rows)
      throws Exception {
    var root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED, false);
    harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
  }

  private static List<String> drain(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<String> values = new ArrayList<>();
    for (Object element : harness.getOutput()) {
      if (!(element instanceof StreamRecord)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
        for (int row = 0; row < root.getRowCount(); row++) {
          List<String> columns = new ArrayList<>();
          for (var vector : root.getFieldVectors())
            columns.add(String.valueOf(vector.getObject(row)));
          values.add(String.join(":", columns));
        }
      }
    }
    harness.getOutput().clear();
    return values;
  }
}
