package io.github.jordepic.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchSerializer;
import io.github.jordepic.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.junit.jupiter.api.Test;

/**
 * The group aggregate on the Paimon state backend: state lives in a local Paimon table, snapshots
 * go through the keyed-state backend as {@link IncrementalRemoteKeyedStateHandle}s (not raw keyed
 * state), a completed checkpoint's files are referenced by placeholders instead of re-uploaded
 * (incremental), and a fresh operator restored from the handle continues the changelog exactly.
 */
class PaimonStateBackendOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "v"});
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key0", "result0"});

  @Test
  void checkpointsIncrementallyAndRestores() throws Exception {
    OperatorSubtaskState second;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      assertEquals(List.of(insert(1, 10), insert(2, 20)), collect(harness));

      OperatorSubtaskState first = harness.snapshot(1, 1);
      IncrementalRemoteKeyedStateHandle firstHandle = paimonHandle(first);
      assertTrue(firstHandle.getSharedState().size() > 0, "first checkpoint uploads data files");
      assertTrue(
          firstHandle.getSharedState().stream()
              .noneMatch(f -> f.getHandle() instanceof PlaceholderStreamStateHandle),
          "nothing to reuse on the first checkpoint");
      harness.notifyOfCompletedCheckpoint(1);

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      assertEquals(
          List.of(update(RowKind.UPDATE_BEFORE, 1, 10), update(RowKind.UPDATE_AFTER, 1, 15)),
          collect(harness));

      second = harness.snapshot(2, 2);
      IncrementalRemoteKeyedStateHandle secondHandle = paimonHandle(second);
      List<HandleAndLocalPath> reused = new ArrayList<>();
      for (HandleAndLocalPath file : secondHandle.getSharedState()) {
        if (file.getHandle() instanceof PlaceholderStreamStateHandle) {
          reused.add(file);
        }
      }
      assertTrue(
          !reused.isEmpty(),
          "the second checkpoint must reference the first checkpoint's files with placeholders");

      // What the checkpoint coordinator does on completion: registering both checkpoints with the
      // shared-state registry resolves the second's placeholders to the first's real handles.
      SharedStateRegistryImpl registry = new SharedStateRegistryImpl();
      firstHandle.registerSharedStates(registry, 1);
      secondHandle.registerSharedStates(registry, 2);
      assertTrue(
          secondHandle.getSharedState().stream()
              .noneMatch(f -> f.getHandle() instanceof PlaceholderStreamStateHandle),
          "registration must resolve every placeholder");
    }

    // A fresh operator restored from the second checkpoint continues the changelog: the restored
    // sums (1 -> 15, 2 -> 20) are the update-before values of the next changes.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(new PaimonStateBackend());
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

  /** Retracting a group to zero records deletes it in the table, across a checkpoint. */
  @Test
  void deletesSurviveCheckpointAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, row(7, 70))));
      collect(harness);
      OperatorSubtaskState first = harness.snapshot(1, 1);
      harness.notifyOfCompletedCheckpoint(1);

      VectorSchemaRoot retract =
          RowDataArrowConverter.write(
              List.of(rowOfKind(RowKind.DELETE, 7, 70)), INPUT, allocator, true);
      harness.processElement(new StreamRecord<>(new ArrowBatch(retract)));
      assertEquals(List.of(update(RowKind.DELETE, 7, 70)), collect(harness));
      snapshot = harness.snapshot(2, 2);
      SharedStateRegistryImpl registry = new SharedStateRegistryImpl();
      paimonHandle(first).registerSharedStates(registry, 1);
      paimonHandle(snapshot).registerSharedStates(registry, 2);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // The deleted key is gone: a new row for it is a plain insert.
      harness.processElement(new StreamRecord<>(batch(allocator, row(7, 1))));
      assertEquals(List.of(insert(7, 1)), collect(harness));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness()
      throws Exception {
    NativeColumnarGroupAggregateOperator operator =
        new NativeColumnarGroupAggregateOperator(
            new int[] {0}, // SUM
            new int[] {0}, // BIGINT
            new int[] {1},
            new int[] {0},
            new int[] {-1},
            new int[] {-1},
            new int[] {-1},
            -1,
            true,
            false,
            0,
            new int[] {-1},
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static IncrementalRemoteKeyedStateHandle paimonHandle(OperatorSubtaskState state) {
    assertEquals(1, state.getManagedKeyedState().size(), "one keyed state handle per checkpoint");
    KeyedStateHandle handle = state.getManagedKeyedState().iterator().next();
    return assertInstanceOf(IncrementalRemoteKeyedStateHandle.class, handle);
  }

  private static RowData row(long key, long value) {
    return GenericRowData.of(key, value);
  }

  private static RowData rowOfKind(RowKind kind, long key, long value) {
    GenericRowData row = GenericRowData.of(key, value);
    row.setRowKind(kind);
    return row;
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
