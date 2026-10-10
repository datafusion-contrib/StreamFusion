package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeColumnarFirstNOperatorTest {
  @org.junit.jupiter.api.BeforeAll
  static void initializeMemory() {
    Configuration config = new Configuration();
    config.set(
        org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
        org.apache.flink.configuration.MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesPerKeyArrivalOrderAcrossBatches(boolean rocks) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(0, 1, 0, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      push(
          harness,
          allocator,
          GenericRowData.of(1L, 30L),
          GenericRowData.of(null, 90L),
          GenericRowData.of(2L, 50L));
      assertEquals(
          List.of(
              Arrays.asList(1L, 30L, 1L), Arrays.asList(null, 90L, 1L), Arrays.asList(2L, 50L, 1L)),
          collect(harness));
      push(
          harness,
          allocator,
          GenericRowData.of(1L, 20L),
          GenericRowData.of(1L, 10L),
          GenericRowData.of(2L, null),
          GenericRowData.of(null, 80L),
          GenericRowData.of(null, 70L));
      assertEquals(
          List.of(
              Arrays.asList(1L, 20L, 2L),
              Arrays.asList(2L, null, 2L),
              Arrays.asList(null, 80L, 2L)),
          collect(harness));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectedRowsDoNotRefreshTtlAndAcceptedRowsDo(boolean rocks) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(1000, 1, 0, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      push(harness, allocator, GenericRowData.of(1L, 10L));
      assertEquals(List.of(List.of(1L, 10L, 1L)), collect(harness));
      harness.setProcessingTime(5500);
      push(harness, allocator, GenericRowData.of(1L, 20L));
      assertEquals(List.of(List.of(1L, 20L, 2L)), collect(harness));
      harness.setProcessingTime(6499);
      push(harness, allocator, GenericRowData.of(1L, 30L));
      assertEquals(List.of(), collect(harness));
      harness.setProcessingTime(6500);
      push(harness, allocator, GenericRowData.of(1L, 40L));
      assertEquals(List.of(List.of(1L, 40L, 1L)), collect(harness));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checkpointPreservesCounterAndTtl(boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(1000, 1, 0, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      push(
          harness,
          allocator,
          GenericRowData.of(1L, 10L),
          GenericRowData.of(2L, 20L),
          GenericRowData.of(2L, 21L));
      collect(harness);
      snapshot = harness.snapshot(1, 1);
    }
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(1000, 1, 0, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(5999);
      push(harness, allocator, GenericRowData.of(1L, 11L), GenericRowData.of(2L, 22L));
      assertEquals(List.of(List.of(1L, 11L, 2L)), collect(harness));
      harness.setProcessingTime(6000);
      push(harness, allocator, GenericRowData.of(1L, 12L), GenericRowData.of(2L, 23L));
      assertEquals(List.of(List.of(2L, 23L, 1L)), collect(harness));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checkpointRescalesCountersByFlinkKeyGroup(boolean rocks) throws Exception {
    long[] keys = new long[2];
    for (int target = 0; target < 2; target++) {
      for (long key = 0; key < 10_000; key++) {
        if (destination(key) == target) {
          keys[target] = key;
          break;
        }
      }
    }
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(0, 1, 0, rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      push(harness, allocator, GenericRowData.of(keys[0], 10L), GenericRowData.of(keys[1], 20L));
      collect(harness);
      snapshot = harness.snapshot(1, 1);
    }
    for (int subtask = 0; subtask < 2; subtask++) {
      try (BufferAllocator allocator = new RootAllocator();
          var harness = harness(0, 2, subtask, rocks)) {
        harness.setup(new ArrowBatchSerializer());
        harness.initializeState(
            AbstractStreamOperatorTestHarness.repartitionOperatorState(
                snapshot, MAX_PARALLELISM, 1, 2, subtask));
        harness.open();
        long key = keys[subtask];
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(
                        List.of(GenericRowData.of(key, 30L), GenericRowData.of(key, 40L)),
                        INPUT,
                        allocator),
                    subtask)));
        assertEquals(List.of(List.of(key, 30L, 2L)), collect(harness));
      }
    }
  }

  private static int destination(long key) {
    int hash =
        new RowDataSerializer(RowType.of(new BigIntType()))
            .toBinaryRow(GenericRowData.of(key))
            .hashCode();
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
        MAX_PARALLELISM,
        2,
        KeyGroupRangeAssignment.computeKeyGroupForKeyHash(hash, MAX_PARALLELISM));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      long ttl, int parallelism, int subtask, boolean rocks) throws Exception {
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new NativeColumnarFirstNOperator(
                new int[] {0}, new int[] {-1}, 2, true, ttl, MAX_PARALLELISM),
            (ArrowBatch batch) -> stateKeys[Math.max(0, batch.destination())],
            Types.INT,
            MAX_PARALLELISM,
            parallelism,
            subtask);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeColumnarFirstNOperatorTest.class.getClassLoader()));
    return harness;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h,
      BufferAllocator allocator,
      RowData... rows)
      throws Exception {
    h.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator))));
  }

  private static List<List<Long>> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h) {
    List<List<Long>> rows = new ArrayList<>();
    while (!h.getOutput().isEmpty()) {
      Object event = h.getOutput().poll();
      if (event instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) event);

        try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(
                Arrays.asList(
                    row.isNullAt(0) ? null : row.getLong(0),
                    row.isNullAt(1) ? null : row.getLong(1),
                    row.getLong(2)));
          }
        }
      }
    }
    return rows;
  }
}
