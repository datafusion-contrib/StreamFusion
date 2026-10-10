package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeFixedOffsetWindowOperatorsTest {
  private static final RowType INPUT =
      RowType.of(
          new BigIntType(),
          new TimestampType(3),
          new TimestampType(3),
          new LocalZonedTimestampType(3));

  @BeforeAll
  static void configureOffHeapBudget() {
    Configuration config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> offsetsAndClocks() {
    return Stream.of(0L, 28_800_000L, -19_800_000L)
        .flatMap(
            offset ->
                Stream.of(false, true)
                    .flatMap(
                        proctime ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(offset, proctime, rocks))));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 28_800_000, -19_800_000})
  void proctimeTvfRendersLocalBoundsAndInstantWindowTime(long offset) throws Exception {
    RowType input = RowType.of(new BigIntType(), new LocalZonedTimestampType(3));
    try (var harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeWindowTableFunctionOperator(1, 1000, 1000, false, true, offset))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(7L, null)), input, NativeAllocator.SHARED))));
      int emitted = 0;
      while (!harness.getOutput().isEmpty()) {
        Object recordCandidate;
        if ((recordCandidate = harness.getOutput().poll()) instanceof StreamRecord<?>) {
          StreamRecord<?> record = ((StreamRecord<?>) recordCandidate);

          try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
            emitted += root.getRowCount();
            assertEquals(1, root.getRowCount());
            assertEquals(1, root.getVector(1).getNullCount());
            assertEquals(offset, new TimestampAccessor(root.getVector(2)).getMillis(0));
            assertEquals(offset + 1000, new TimestampAccessor(root.getVector(3)).getMillis(0));
            assertEquals(999, new TimestampAccessor(root.getVector(4)).getMillis(0));
          }
        }
      }
      assertEquals(1, emitted);
    }
  }

  @ParameterizedTest
  @MethodSource("offsetsAndClocks")
  void rankFiresAtLastMillisecondAfterRecovery(long offset, boolean proctime, boolean rocks)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (var before = rank(offset, proctime, rocks)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      assertEquals(
          rocks, ((AbstractNativeStatefulOperator<?>) before.getOperator()).directRocksDBState());
      before.setProcessingTime(500);
      before.processElement(new StreamRecord<>(batch(offset, 1)));
      before.processElement(new StreamRecord<>(batch(offset, 2)));
      if (proctime) before.setProcessingTime(998);
      else before.processWatermark(new Watermark(998));
      assertEquals(List.of(), collect(before.getOutput()));
      snapshot = before.snapshot(1, 1);
    }
    try (var restored = rank(offset, proctime, rocks)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      assertEquals(
          rocks, ((AbstractNativeStatefulOperator<?>) restored.getOperator()).directRocksDBState());
      if (proctime) restored.setProcessingTime(999);
      else restored.processWatermark(new Watermark(999));
      assertEquals(
          List.of(List.of(2L, offset, offset + 1000, 999L)), collect(restored.getOutput()));
      if (!proctime) {
        restored.processElement(new StreamRecord<>(batch(offset, 9)));
        restored.processWatermark(new Watermark(Long.MAX_VALUE));
        assertEquals(List.of(), collect(restored.getOutput()));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("offsetsAndClocks")
  void joinFiresAtLastMillisecondAfterRecovery(long offset, boolean proctime, boolean rocks)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (var before = join(offset, proctime, rocks)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      assertEquals(
          rocks, ((AbstractNativeStatefulOperator<?>) before.getOperator()).directRocksDBState());
      before.setProcessingTime(500);
      before.processElement1(new StreamRecord<>(batch(offset, 7)));
      before.processElement2(new StreamRecord<>(batch(offset, 7)));
      if (proctime) before.setProcessingTime(998);
      else {
        before.processWatermark1(new Watermark(998));
        before.processWatermark2(new Watermark(998));
      }
      assertEquals(List.of(), collect(before.getOutput()));
      snapshot = before.snapshot(1, 1);
    }
    try (var restored = join(offset, proctime, rocks)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      assertEquals(
          rocks, ((AbstractNativeStatefulOperator<?>) restored.getOperator()).directRocksDBState());
      if (proctime) restored.setProcessingTime(999);
      else {
        restored.processWatermark1(new Watermark(999));
        restored.processWatermark2(new Watermark(999));
      }
      assertEquals(
          List.of(List.of(7L, offset, offset + 1000, 999L)), collect(restored.getOutput()));
      if (!proctime) {
        restored.processElement1(new StreamRecord<>(batch(offset, 9)));
        restored.processElement2(new StreamRecord<>(batch(offset, 9)));
        restored.processWatermark1(new Watermark(Long.MAX_VALUE));
        restored.processWatermark2(new Watermark(Long.MAX_VALUE));
        assertEquals(List.of(), collect(restored.getOutput()));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void processingTimeRankDoesNotFireOnBoundedCompletion(boolean rocks) throws Exception {
    try (var harness = rank(0, true, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(batch(0, 7)));
      harness.processWatermark(Watermark.MAX_WATERMARK);
      harness.getOperator().finish();
      assertEquals(List.of(), collect(harness.getOutput()));
      harness.setProcessingTime(999);
      assertEquals(List.of(List.of(7L, 0L, 1000L, 999L)), collect(harness.getOutput()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void processingTimeJoinDoesNotFireOnBoundedCompletion(boolean rocks) throws Exception {
    try (var harness = join(0, true, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement1(new StreamRecord<>(batch(0, 7)));
      harness.processElement2(new StreamRecord<>(batch(0, 7)));
      harness.processWatermark1(Watermark.MAX_WATERMARK);
      harness.processWatermark2(Watermark.MAX_WATERMARK);
      harness.getOperator().finish();
      assertEquals(List.of(), collect(harness.getOutput()));
      harness.setProcessingTime(999);
      assertEquals(List.of(List.of(7L, 0L, 1000L, 999L)), collect(harness.getOutput()));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> rank(
      long offset, boolean proctime, boolean rocks) throws Exception {
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            new NativeColumnarWindowRankOperator(
                1,
                2,
                new int[0],
                new int[0],
                new int[] {0},
                new int[] {0},
                new int[] {0},
                1,
                false,
                offset,
                proctime,
                1000,
                1000,
                false,
                INPUT,
                128),
            batch -> 0,
            Types.INT,
            128,
            1,
            0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(new Configuration(), getClassLoader()));
    return harness;
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      join(long offset, boolean proctime, boolean rocks) throws Exception {
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
            new NativeWindowJoinOperator(
                new int[] {0},
                new int[] {0},
                1,
                2,
                1,
                2,
                0,
                INPUT,
                INPUT,
                EncodedPredicate.NONE,
                proctime,
                1000,
                1000,
                false,
                new int[] {-1},
                128,
                offset),
            batch -> 0,
            batch -> 0,
            Types.INT,
            128,
            1,
            0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(new Configuration(), getClassLoader()));
    return harness;
  }

  private static ClassLoader getClassLoader() {
    return NativeFixedOffsetWindowOperatorsTest.class.getClassLoader();
  }

  private static ArrowBatch batch(long offset, long id) {
    return new ArrowBatch(
        RowDataArrowConverter.write(
            List.of(
                GenericRowData.of(
                    id,
                    TimestampData.fromEpochMillis(offset),
                    TimestampData.fromEpochMillis(offset + 1000),
                    TimestampData.fromEpochMillis(999))),
            INPUT,
            NativeAllocator.SHARED));
  }

  private static List<List<Long>> collect(Queue<Object> output) {
    List<List<Long>> rows = new ArrayList<>();
    while (!output.isEmpty()) {
      Object recordCandidate2;
      if ((recordCandidate2 = output.poll()) instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) recordCandidate2);

        try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            rows.add(
                List.of(
                    ((BigIntVector) root.getVector(0)).get(i),
                    new TimestampAccessor(root.getVector(1)).getMillis(i),
                    new TimestampAccessor(root.getVector(2)).getMillis(i),
                    new TimestampAccessor(root.getVector(3)).getMillis(i)));
          }
        }
      }
    }
    return rows;
  }
}
