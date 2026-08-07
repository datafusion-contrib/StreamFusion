package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.CoalescingOff;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Direct-RocksDB checkpoint, incremental reuse, restore, and TTL coverage. */
@ExtendWith(CoalescingOff.class)
class RocksDBNativeStateBackendOperatorTest {

  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "v"});
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key0", "result0"});

  @BeforeAll
  static void initializeTaskOffHeapAuthority() {
    Configuration configuration = new Configuration();
    configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(configuration);
  }

  @Test
  void checkpointsIncrementallyAndRestores() throws Exception {
    OperatorSubtaskState second;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(0)) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      assertEquals(List.of(insert(1, 10), insert(2, 20)), collect(harness));

      OperatorSubtaskState first = harness.snapshot(1, 1);
      IncrementalRemoteKeyedStateHandle firstHandle = nativeHandle(first);
      assertTrue(!firstHandle.getSharedState().isEmpty());
      harness.notifyOfCompletedCheckpoint(1);

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      assertEquals(
          List.of(update(RowKind.UPDATE_BEFORE, 1, 10), update(RowKind.UPDATE_AFTER, 1, 15)),
          collect(harness));
      second = harness.snapshot(2, 2);
      IncrementalRemoteKeyedStateHandle secondHandle = nativeHandle(second);
      List<HandleAndLocalPath> reused = new ArrayList<>();
      for (HandleAndLocalPath file : secondHandle.getSharedState()) {
        if (file.getHandle() instanceof PlaceholderStreamStateHandle) {
          reused.add(file);
        }
      }
      assertTrue(!reused.isEmpty(), "the second checkpoint reuses immutable SST files");
      SharedStateRegistryImpl registry = new SharedStateRegistryImpl();
      firstHandle.registerSharedStates(registry, 1);
      secondHandle.registerSharedStates(registry, 2);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(0)) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(second);
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 100), row(2, 7))));
      assertEquals(
          List.of(
              update(RowKind.UPDATE_BEFORE, 1, 15),
              update(RowKind.UPDATE_AFTER, 1, 115),
              update(RowKind.UPDATE_BEFORE, 2, 20),
              update(RowKind.UPDATE_AFTER, 2, 27)),
          collect(harness));
    }
  }

  @Test
  void ttlExpiresAcrossCheckpointAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(1000)) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      collect(harness);
      snapshot = harness.snapshot(1, 1);
      nativeHandle(snapshot);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(1000)) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(6000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      assertEquals(List.of(insert(1, 5)), collect(harness));
    }
  }

  @Test
  void disablingIncrementalCheckpointsUploadsEverySst() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(0)) {
      harness.setStateBackend(backend(false));
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      collect(harness);
      harness.snapshot(1, 1);
      harness.notifyOfCompletedCheckpoint(1);
      IncrementalRemoteKeyedStateHandle second = nativeHandle(harness.snapshot(2, 2));
      assertTrue(
          second.getSharedState().stream()
              .noneMatch(file -> file.getHandle() instanceof PlaceholderStreamStateHandle),
          "execution.checkpointing.incremental=false must not reuse prior SST handles");
    }
  }

  private static RocksDBNativeStateBackend backend() {
    return backend(true);
  }

  private static RocksDBNativeStateBackend backend(boolean incremental) {
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);
    return new RocksDBNativeStateBackend(
        config, RocksDBNativeStateBackendOperatorTest.class.getClassLoader());
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      long ttl) throws Exception {
    NativeColumnarGroupAggregateOperator operator =
        new NativeColumnarGroupAggregateOperator(
            new int[] {0}, new int[] {0}, new int[] {1}, new int[] {0}, new int[] {-1},
            new int[] {-1}, new int[] {-1}, -1, true, false, 0, ttl, new int[] {-1},
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static IncrementalRemoteKeyedStateHandle nativeHandle(OperatorSubtaskState state) {
    assertEquals(1, state.getManagedKeyedState().size());
    KeyedStateHandle handle = state.getManagedKeyedState().iterator().next();
    return assertInstanceOf(IncrementalRemoteKeyedStateHandle.class, handle);
  }

  private static RowData row(long key, long value) {
    return GenericRowData.of(key, value);
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  private static List<List<Object>> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(row.getRowKind(), row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    return rows;
  }

  private static List<Object> insert(long key, long total) {
    return List.of(RowKind.INSERT, key, total);
  }

  private static List<Object> update(RowKind kind, long key, long total) {
    return List.of(kind, key, total);
  }
}
