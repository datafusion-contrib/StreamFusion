package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.EncodedPredicate;
import tech.streamfusion.operator.NativeColumnarChangelogNormalizeOperator;
import tech.streamfusion.operator.NativeColumnarDeduplicateOperator;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.NativeColumnarKeepLastDeduplicateOperator;
import tech.streamfusion.operator.NativeColumnarSessionWindowAggregateOperator;
import tech.streamfusion.operator.NativeColumnarTemporalSortOperator;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.operator.NativeColumnarUpdatingJoinOperator;
import tech.streamfusion.operator.NativeColumnarWindowAggregateOperator;
import tech.streamfusion.operator.NativeColumnarWindowRankOperator;
import tech.streamfusion.operator.NativeIntervalJoinOperator;
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.operator.NativeTemporalJoinOperator;
import tech.streamfusion.operator.NativeWindowJoinOperator;
import tech.streamfusion.operator.NativeStateRouteProbe;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.streamfusion.operator.CoalescingOff;

/**
 * Native operators on the RocksDB state backend: state lives in a local RocksDB table, snapshots go
 * through the keyed-state backend as {@link IncrementalRemoteKeyedStateHandle}s (not raw keyed
 * state), a completed checkpoint's files are referenced by placeholders instead of re-uploaded
 * (incremental), and a fresh operator restored from the handle continues the changelog exactly.
 *
 * <p>Every run here is the production shape: Rust reads and writes its RocksDB instance directly,
 * while Java coordinates Flink checkpoint handles and uploads.
 *
 * <p>State-transition cases run through an ordinary RocksDB checkpoint and through canonical
 * savepoints in both backend directions. This keeps the incremental lifecycle assertions while
 * applying the same semantic continuation checks to memory-to-RocksDB and RocksDB-to-memory
 * restores.
 */
@ExtendWith(CoalescingOff.class)
class RocksDBNativeStateBackendAllOperatorsTest {

  private static final int MAX_PARALLELISM = 128;

  @BeforeAll
  static void initializeTaskOffHeapAuthority() {
    Configuration configuration = new Configuration();
    configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(configuration);
  }

  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "v"});
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key0", "result0"});

  private static final RowType DEDUP_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"k", "v", "rt"});

  private static final RowType WINDOW_INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"value", "rt"});

  private static final RowType WINDOW_OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new TimestampType(3), new TimestampType(3)},
          new String[] {"total", "window_start", "window_end"});

  /**
   * A proctime tumbling window keeps its direct RocksDB store: the firing deadline rides the typed
   * store's reserved key, so after a checkpoint/restore cycle the restored deadline alone re-arms
   * the processing-time timer that closes the window — no raw keyed state, no snapshot-store blob.
   */
  @Test
  void proctimeWindowAggregateRearmsTimerFromTypedStore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            proctimeWindowHarness()) {
      before.setStateBackend(backend());
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(500);
      before.processElement(new StreamRecord<>(windowBatch(allocator, 6)));
      snapshot = before.snapshot(1, 1);
      rocksHandle(snapshot);
      collectWindows(before);
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
        proctimeWindowHarness()) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      // No input after recovery: the typed store's restored deadline alone closes the window.
      restored.setProcessingTime(1000);
      assertEquals(List.of(List.of(6L, 0L, 1000L)), collectWindows(restored));
    }
  }

  /**
   * A windowed operator's canonical restore also imports into its typed store: the restored
   * event-time window aggregate provably runs direct, the imported open window fires on the next
   * watermark, and a checkpoint taken after the import round-trips through a second restore.
   */
  @Test
  void canonicalRestoreImportsWindowAggregateIntoDirectStore() throws Exception {
    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            eventTimeWindowHarness(eventTimeWindowOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement(new StreamRecord<>(windowBatch(allocator, 6)));
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarWindowAggregateOperator imported = eventTimeWindowOperator();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            eventTimeWindowHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      restored.processElement(new StreamRecord<>(windowBatch(allocator, 4)));
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
        eventTimeWindowHarness(eventTimeWindowOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(List.of(10L, 0L, 1000L)), collectWindows(harness));
    }
  }

  /**
   * A window join's canonical restore imports both sides' buffered rows into the typed store: the
   * restored operator provably runs direct, the imported rows join on the closing watermark exactly
   * as an uninterrupted memory run does, and a checkpoint taken after the import round-trips.
   * The savepoint predates any watermark, so nothing depends on the (unpersisted) watermark.
   */
  @Test
  void canonicalRestoreImportsWindowJoinIntoDirectStore() throws Exception {
    List<List<Long>> expected;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            reference = windowJoinHarness(windowJoinOperator())) {
      reference.setup(new ArrowBatchSerializer());
      reference.open();
      reference.processElement1(new StreamRecord<>(windowJoinBatch(allocator, 1, 10)));
      reference.processElement2(new StreamRecord<>(windowJoinBatch(allocator, 1, 100)));
      reference.processBothWatermarks(new Watermark(1000));
      expected = collectWindowJoinPairs(reference);
      assertEquals(List.of(List.of(1L, 10L, 1L, 100L)), expected);
    }

    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            memory = windowJoinHarness(windowJoinOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement1(new StreamRecord<>(windowJoinBatch(allocator, 1, 10)));
      memory.processElement2(new StreamRecord<>(windowJoinBatch(allocator, 1, 100)));
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeWindowJoinOperator imported = windowJoinOperator();
    try (KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
        restored = windowJoinHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
      restored.processBothWatermarks(new Watermark(1000));
      assertEquals(expected, collectWindowJoinPairs(restored));
    }

    try (KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
        harness = windowJoinHarness(windowJoinOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processBothWatermarks(new Watermark(1000));
      assertEquals(expected, collectWindowJoinPairs(harness));
    }
  }

  /**
   * An interval join's canonical restore imports the buffered rows (and their outer-join row-id
   * identity) into the typed store: the restored operator provably runs direct, a post-restore
   * probe row still matches the imported buffered row, and the checkpoint taken right after the
   * import round-trips. No watermark is taken before the savepoint, so nothing depends on the
   * (unpersisted) watermark.
   */
  @Test
  void canonicalRestoreImportsIntervalJoinIntoDirectStore() throws Exception {
    List<List<Object>> expected;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            reference = intervalJoinHarness(intervalJoinOperator())) {
      reference.setup(new ArrowBatchSerializer());
      reference.open();
      reference.processElement1(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L, 5000L)), TEMPORAL_ROW, allocator))));
      collectTemporal(reference);
      reference.processElement2(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 100L, 5500L)), TEMPORAL_ROW, allocator))));
      expected = collectTemporal(reference);
      assertEquals(List.of(temporalRow(1L, 10L, 5000L, 1L, 100L, 5500L)), expected);
    }

    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            memory = intervalJoinHarness(intervalJoinOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement1(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L, 5000L)), TEMPORAL_ROW, allocator))));
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeIntervalJoinOperator imported = intervalJoinOperator();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            restored = intervalJoinHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
      restored.processElement2(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 100L, 5500L)), TEMPORAL_ROW, allocator))));
      assertEquals(expected, collectTemporal(restored));
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = intervalJoinHarness(intervalJoinOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processElement2(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 100L, 5500L)), TEMPORAL_ROW, allocator))));
      assertEquals(expected, collectTemporal(harness));
    }
  }

  /**
   * A window rank's canonical restore imports the open windows' buffered rows and the late-data
   * watermark into the typed store: the restored operator provably runs direct, post-restore rows
   * re-rank against the imported buffer, the closing watermark emits exactly the uninterrupted
   * memory run's top-N, and a checkpoint taken after the import round-trips.
   */
  @Test
  void canonicalRestoreImportsWindowRankIntoDirectStore() throws Exception {
    List<List<Long>> expected;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> reference =
            windowRankHarness(windowRankOperator())) {
      reference.setup(new ArrowBatchSerializer());
      reference.open();
      reference.processElement(
          new StreamRecord<>(rankBatch(allocator, rankRow(10), rankRow(30))));
      reference.processElement(new StreamRecord<>(rankBatch(allocator, rankRow(20))));
      reference.processWatermark(new Watermark(1000));
      expected = collectRanked(reference);
      assertEquals(List.of(List.of(30L, 1L), List.of(20L, 2L)), expected);
    }

    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            windowRankHarness(windowRankOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement(new StreamRecord<>(rankBatch(allocator, rankRow(10), rankRow(30))));
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarWindowRankOperator imported = windowRankOperator();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            windowRankHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      restored.processElement(new StreamRecord<>(rankBatch(allocator, rankRow(20))));
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
      restored.processWatermark(new Watermark(1000));
      assertEquals(expected, collectRanked(restored));
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
        windowRankHarness(windowRankOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processWatermark(new Watermark(1000));
      assertEquals(expected, collectRanked(harness));
    }
  }

  /**
   * A session aggregate's canonical restore imports the open sessions into the typed store: the
   * restored operator provably runs direct, a post-restore row merges into the imported session,
   * the closing watermark emits exactly the uninterrupted memory run's session, and a checkpoint
   * taken after the import round-trips. The savepoint predates any watermark, so nothing depends
   * on the (unpersisted) watermark.
   */
  @Test
  void canonicalRestoreImportsSessionAggregateIntoDirectStore() throws Exception {
    List<List<Long>> expected;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> reference =
            sessionHarness(sessionOperator())) {
      reference.setup(new ArrowBatchSerializer());
      reference.open();
      reference.processElement(new StreamRecord<>(sessionBatch(allocator, 10, 0)));
      reference.processElement(new StreamRecord<>(sessionBatch(allocator, 20, 200)));
      reference.processWatermark(new Watermark(700));
      expected = collectSessions(reference);
      assertEquals(List.of(List.of(30L, 0L, 700L)), expected);
    }

    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            sessionHarness(sessionOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement(new StreamRecord<>(sessionBatch(allocator, 10, 0)));
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarSessionWindowAggregateOperator imported = sessionOperator();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            sessionHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      restored.processElement(new StreamRecord<>(sessionBatch(allocator, 20, 200)));
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
      restored.processWatermark(new Watermark(700));
      assertEquals(expected, collectSessions(restored));
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
        sessionHarness(sessionOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processWatermark(new Watermark(700));
      assertEquals(expected, collectSessions(harness));
    }
  }

  /**
   * A keep-first deduplicate's canonical restore imports the pending candidates, fired markers,
   * AND the late-data watermark (which this blob carries) into the typed store: the restored
   * operator provably runs direct, an already-fired key stays suppressed, a row below the imported
   * watermark drops as late, the remaining input finishes exactly as the uninterrupted memory run
   * does, and a checkpoint taken after the import round-trips.
   */
  @Test
  void canonicalRestoreImportsKeepFirstDedupIntoDirectStore() throws Exception {
    List<List<Long>> expected;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> reference =
            keepFirstHarness(keepFirstOperator())) {
      reference.setup(new ArrowBatchSerializer());
      reference.open();
      keepFirstPhaseOne(reference, allocator);
      collectDedup(reference);
      expected = keepFirstPhaseTwo(reference, allocator);
      assertEquals(List.of(List.of(2L, 40L), List.of(3L, 8L)), expected);
    }

    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            keepFirstHarness(keepFirstOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      keepFirstPhaseOne(memory, allocator);
      collectDedup(memory);
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarDeduplicateOperator imported = keepFirstOperator();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            keepFirstHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
      assertEquals(expected, keepFirstPhaseTwo(restored, allocator));
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keepFirstHarness(keepFirstOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      assertEquals(expected, keepFirstPhaseTwo(harness, allocator));
    }
  }

  /**
   * A temporal sorter's canonical restore imports the buffered rows into the typed store under
   * fresh arrival sequences: the restored operator provably runs direct, later watermarks release
   * the imported and post-restore rows in exactly the uninterrupted memory run's order, and a
   * checkpoint taken after the import round-trips.
   */
  @Test
  void canonicalRestoreImportsTemporalSorterIntoDirectStore() throws Exception {
    List<List<Long>> expected;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> reference =
            sorterHarness(new NativeColumnarTemporalSortOperator(1, SORT_ROW))) {
      reference.setup(new ArrowBatchSerializer());
      reference.open();
      reference.processElement(
          new StreamRecord<>(sortBatch(allocator, sortRow(30, 2000), sortRow(10, 500))));
      reference.processElement(new StreamRecord<>(sortBatch(allocator, sortRow(20, 0))));
      reference.processWatermark(new Watermark(3000));
      expected = collectSorted(reference);
      assertEquals(List.of(List.of(20L, 0L), List.of(10L, 500L), List.of(30L, 2000L)), expected);
    }

    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            sorterHarness(new NativeColumnarTemporalSortOperator(1, SORT_ROW))) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement(
          new StreamRecord<>(sortBatch(allocator, sortRow(30, 2000), sortRow(10, 500))));
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarTemporalSortOperator imported = new NativeColumnarTemporalSortOperator(1, SORT_ROW);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            sorterHarness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      restored.processElement(new StreamRecord<>(sortBatch(allocator, sortRow(20, 0))));
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
      restored.processWatermark(new Watermark(3000));
      assertEquals(expected, collectSorted(restored));
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
        sorterHarness(new NativeColumnarTemporalSortOperator(1, SORT_ROW))) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processWatermark(new Watermark(3000));
      assertEquals(expected, collectSorted(harness));
    }
  }

  private static NativeColumnarWindowAggregateOperator eventTimeWindowOperator() {
    return new NativeColumnarWindowAggregateOperator(
        false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0},
        new int[] {0}, "UTC", WINDOW_OUTPUT, false, new int[0], MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      eventTimeWindowHarness(NativeColumnarWindowAggregateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      proctimeWindowHarness() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0},
            new int[] {0}, "UTC", WINDOW_OUTPUT, true, new int[0], MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static ArrowBatch windowBatch(BufferAllocator allocator, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(0));
    return new ArrowBatch(RowDataArrowConverter.write(List.of(row), WINDOW_INPUT, allocator));
  }

  private static List<List<Long>> collectWindows(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, WINDOW_OUTPUT)) {
            rows.add(
                List.of(
                    row.getLong(0),
                    row.getTimestamp(1, 3).getMillisecond(),
                    row.getTimestamp(2, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }

  @Test
  void checkpointsIncrementallyAndRestores() throws Exception {
    OperatorSubtaskState second;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      assertEquals(List.of(insert(1, 10), insert(2, 20)), collect(harness));

      OperatorSubtaskState first = harness.snapshot(1, 1);
      IncrementalRemoteKeyedStateHandle firstHandle = rocksHandle(first);
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
      IncrementalRemoteKeyedStateHandle secondHandle = rocksHandle(second);
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

  /**
   * A TTL'd group aggregate rides the RocksDB route too (its snapshot is an incremental RocksDB
   * handle, not raw keyed state): the last-write timestamp persists absolutely in the table's
   * trailing ts column, so after restore the key expires at write-time + ttl and its next row
   * is a fresh insert.
   */
  @Test
  void groupAggregateTtlExpiresAcrossCheckpointAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(1000)) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      assertEquals(List.of(insert(1, 10)), collect(harness));
      snapshot = harness.snapshot(1, 1);
      rocksHandle(snapshot); // the TTL'd aggregate must resolve to the RocksDB route
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(1000)) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // 5000 + 1000 <= 6000: the restored key hydrates as expired (delete-on-read), so the sum
      // restarts as a fresh +I instead of a -U/+U update.
      harness.setProcessingTime(6000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      assertEquals(List.of(insert(1, 5)), collect(harness));
    }
  }

  /**
   * A retention-bounded event-time OVER rides the RocksDB route: the per-key cleanup deadline
   * persists absolutely in the deadlines table, so after restore the key folds fresh at exactly
   * the writer's deadline. The deadline shapes deliberately register no retention with the
   * backend ({@code resolveRocksDB} without a TTL) — a deferred or re-armed deadline is not a
   * truthful per-row clock, so every maintenance session opens WITHOUT record-level expiry
   * options ({@link #recordLevelExpireOptionsPadTheRetention} pins the zero-retention mapping);
   * physical cleanup is the operator's own staged tombstones.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesOverAggregateRetention(StateTransition transition)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(2000)) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // The element at 5000 arms the key's cleanup deadline at 5000 + 1.5x2000 = 8000.
      harness.setProcessingTime(5000);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L, 100L)), OVER_ROW, allocator))));
      harness.processWatermark(new Watermark(200));
      assertEquals(List.of(List.of(RowKind.INSERT, 1L, 10L, 100L, 10L)), collectOver(harness));
      snapshot = transition.snapshot(harness);
    }

    // One ms inside the restored (absolute) deadline the fold continues from the table...
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(2000)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(7999);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 5L, 300L)), OVER_ROW, allocator))));
      harness.processWatermark(new Watermark(400));
      assertEquals(List.of(List.of(RowKind.INSERT, 1L, 5L, 300L, 15L)), collectOver(harness));
    }

    // ...and at exactly the deadline the key's fold cleared: the running sum restarts fresh.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(2000)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(8000);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 5L, 300L)), OVER_ROW, allocator))));
      harness.processWatermark(new Watermark(400));
      assertEquals(List.of(List.of(RowKind.INSERT, 1L, 5L, 300L, 5L)), collectOver(harness));
    }
  }

  /**
   * A bounded-ROWS OVER frame rides the typed store's frames table: the per-key sliding buffer
   * survives every transition (RocksDB checkpoint, canonical in both backend directions), so the
   * restored row still falls inside the next row's frame, and on the RocksDB target the operator
   * provably runs direct — no snapshot-store blob.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesBoundedRowsOverFrame(StateTransition transition) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(overOperator(new int[] {0}, 1, 1L, false, 0))) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(overBatch(allocator, 10, 100)));
      harness.processElement(new StreamRecord<>(overBatch(allocator, 20, 200)));
      harness.processWatermark(new Watermark(200));
      assertEquals(
          List.of(
              List.of(RowKind.INSERT, 1L, 10L, 100L, 10L),
              List.of(RowKind.INSERT, 1L, 20L, 200L, 30L)),
          collectOver(harness));
      snapshot = transition.snapshot(harness);
    }

    NativeOverAggregateOperator imported = overOperator(new int[] {0}, 1, 1L, false, 0);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(imported)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      if (transition != StateTransition.ROCKSDB_TO_MEMORY) {
        assertTrue(
            NativeStateRouteProbe.directRocksDBState(imported),
            "a bounded OVER frame must restore into the direct typed store");
      }
      // The restored buffered row (v=20) is the one preceding row of the new row's frame.
      harness.processElement(new StreamRecord<>(overBatch(allocator, 5, 300)));
      harness.processWatermark(new Watermark(400));
      assertEquals(List.of(List.of(RowKind.INSERT, 1L, 5L, 300L, 25L)), collectOver(harness));
    }
  }

  /**
   * A proctime OVER (unbounded fold, eager per-row emission) rides the typed store: the running
   * fold and the arrival counter persist, so a restored operator continues the sum exactly, and
   * on the RocksDB target it provably runs direct.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesProctimeOverAggregate(StateTransition transition) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(overOperator(new int[] {0}, 0, 0L, true, 0))) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(overBatch(allocator, 10, 0)));
      harness.processElement(new StreamRecord<>(overBatch(allocator, 5, 0)));
      assertEquals(
          List.of(
              List.of(RowKind.INSERT, 1L, 10L, 0L, 10L),
              List.of(RowKind.INSERT, 1L, 5L, 0L, 15L)),
          collectOver(harness));
      snapshot = transition.snapshot(harness);
    }

    NativeOverAggregateOperator imported = overOperator(new int[] {0}, 0, 0L, true, 0);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(imported)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      if (transition != StateTransition.ROCKSDB_TO_MEMORY) {
        assertTrue(
            NativeStateRouteProbe.directRocksDBState(imported),
            "a proctime OVER must restore into the direct typed store");
      }
      harness.processElement(new StreamRecord<>(overBatch(allocator, 2, 0)));
      assertEquals(List.of(List.of(RowKind.INSERT, 1L, 2L, 0L, 17L)), collectOver(harness));
    }
  }

  /**
   * A DISTINCT OVER aggregate rides the typed store's per-element seen-set tables: an element
   * seen before the transition stays skipped after it, on every transition, and on the RocksDB
   * target the operator provably runs direct.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesDistinctOverAggregate(StateTransition transition) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(overOperator(new int[] {100}, 0, 0L, false, 0))) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(overBatch(allocator, 10, 100)));
      harness.processWatermark(new Watermark(100));
      assertEquals(List.of(List.of(RowKind.INSERT, 1L, 10L, 100L, 10L)), collectOver(harness));
      snapshot = transition.snapshot(harness);
    }

    NativeOverAggregateOperator imported = overOperator(new int[] {100}, 0, 0L, false, 0);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(imported)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      if (transition != StateTransition.ROCKSDB_TO_MEMORY) {
        assertTrue(
            NativeStateRouteProbe.directRocksDBState(imported),
            "a DISTINCT OVER must restore into the direct typed store");
      }
      // 10 was seen before the transition (skipped); 20 is new (folds).
      harness.processElement(new StreamRecord<>(overBatch(allocator, 10, 200)));
      harness.processElement(new StreamRecord<>(overBatch(allocator, 20, 300)));
      harness.processWatermark(new Watermark(400));
      assertEquals(
          List.of(
              List.of(RowKind.INSERT, 1L, 10L, 200L, 10L),
              List.of(RowKind.INSERT, 1L, 20L, 300L, 30L)),
          collectOver(harness));
    }
  }

  /**
   * A retention-bounded temporal join rides the RocksDB route with the same absolute-deadline
   * semantics: past the restored deadline the key's whole state — both sides — is gone, so the
   * probe null-pads exactly as if no version ever existed. See the OVER test above for why the
   * maintenance session gets no record-level expiry options.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesTemporalJoinRetention(StateTransition transition)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = temporalHarness(2000)) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // The build version at 5000 arms the key's cleanup deadline at 5000 + 1.5x2000 = 8000.
      harness.setProcessingTime(5000);
      harness.processElement2(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L, 100L)), TEMPORAL_ROW, allocator, true))));
      snapshot = transition.snapshot(harness);
    }

    // One ms inside the restored (absolute) deadline the probe still joins the version...
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = temporalHarness(2000)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(7999);
      harness.processElement1(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 1L, 200L)), TEMPORAL_ROW, allocator))));
      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      assertEquals(
          List.of(temporalRow(1L, 1L, 200L, 1L, 10L, 100L)), collectTemporal(harness));
    }

    // ...and at exactly the deadline the key's whole state cleared: the LEFT probe null-pads.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = temporalHarness(2000)) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(8000);
      harness.processElement1(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 1L, 200L)), TEMPORAL_ROW, allocator))));
      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      assertEquals(
          List.of(temporalRow(1L, 1L, 200L, null, null, null)), collectTemporal(harness));
    }
  }

  /**
   * A canonical (memory-format) savepoint restored onto the RocksDB backend imports the blob key
   * groups into the operator's typed store once at open: the restored operator provably runs the
   * direct route, continues the changelog from the imported state, and its next checkpoint is an
   * ordinary incremental RocksDB handle that itself restores identically — the import round-trips.
   */
  @Test
  void canonicalRestoreImportsIntoDirectStore() throws Exception {
    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            harness()) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      collect(memory);
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarGroupAggregateOperator imported = groupAggregateOperator(0);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      assertEquals(
          List.of(update(RowKind.UPDATE_BEFORE, 1, 10), update(RowKind.UPDATE_AFTER, 1, 15)),
          collect(restored));
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
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

  /**
   * A multiset aggregate's canonical restore imports the blob side batches into the companion
   * element tables and the running extreme into the main row: the restored operator provably runs
   * the direct route, a retraction of the imported minimum reseeks the imported elements for the
   * next extreme, and the post-import checkpoint round-trips.
   */
  @Test
  void canonicalRestoreImportsMultisetAggregateIntoDirectStore() throws Exception {
    OperatorSubtaskState savepoint;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> memory =
            harness(minAggregateOperator())) {
      memory.setup(new ArrowBatchSerializer());
      memory.open();
      memory.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10), row(1, 5), row(2, 20))));
      collect(memory);
      savepoint = canonicalSavepoint(memory);
    }

    OperatorSubtaskState checkpoint;
    NativeColumnarGroupAggregateOperator imported = minAggregateOperator();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness(imported)) {
      restored.setStateBackend(backend());
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(savepoint);
      restored.open();
      assertTrue(
          NativeStateRouteProbe.directRocksDBState(imported),
          "a canonical restore must import into the direct typed store, not the blob path");
      restored.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(rowOfKind(RowKind.DELETE, 1, 5)), INPUT, allocator, true))));
      assertEquals(
          List.of(update(RowKind.UPDATE_BEFORE, 1, 5), update(RowKind.UPDATE_AFTER, 1, 10)),
          collect(restored));
      checkpoint = restored.snapshot(1, 1);
      rocksHandle(checkpoint);
      restored.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(minAggregateOperator())) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(checkpoint);
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(rowOfKind(RowKind.DELETE, 1, 10), row(2, 5)),
                      INPUT,
                      allocator,
                      true))));
      assertEquals(
          List.of(
              update(RowKind.DELETE, 1, 10),
              update(RowKind.UPDATE_BEFORE, 2, 20),
              update(RowKind.UPDATE_AFTER, 2, 5)),
          collect(harness));
    }
  }

  private static NativeColumnarGroupAggregateOperator minAggregateOperator() {
    return new NativeColumnarGroupAggregateOperator(
        new int[] {1}, // MIN
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
        0,
        new int[] {-1},
        MAX_PARALLELISM);
  }

  /** Retracting a group to zero records deletes it in the table, across a checkpoint. */
  @Test
  void deletesSurviveCheckpointAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(backend());
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
      rocksHandle(first).registerSharedStates(registry, 1);
      rocksHandle(snapshot).registerSharedStates(registry, 2);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setStateBackend(backend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // The deleted key is gone: a new row for it is a plain insert.
      harness.processElement(new StreamRecord<>(batch(allocator, row(7, 1))));
      assertEquals(List.of(insert(7, 1)), collect(harness));
    }
  }

  /**
   * The keep-last deduplicator rides the same backend: its checkpoint is an incremental RocksDB
   * handle, and a restored operator's retraction carries the payload persisted before the restore.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesDedupState(StateTransition transition) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            dedupHarness()) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L, 1L), GenericRowData.of(2L, 20L, 1L)),
                      DEDUP_ROW,
                      allocator))));
      assertEquals(
          List.of(
              List.of(RowKind.INSERT, 1L, 10L, 1L), List.of(RowKind.INSERT, 2L, 20L, 1L)),
          collectDedup(harness));

      snapshot = transition.snapshot(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            dedupHarness()) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 11L, 5L)), DEDUP_ROW, allocator))));
      assertEquals(
          List.of(
              List.of(RowKind.UPDATE_BEFORE, 1L, 10L, 1L),
              List.of(RowKind.UPDATE_AFTER, 1L, 11L, 5L)),
          collectDedup(harness));
    }
  }

  /**
   * The changelog normalizer rides the same backend; a restored operator's delete emits the
   * stored full row (hydrated from the pre-restore table) and tombstones it.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesNormalizerState(StateTransition transition) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            normalizerHarness()) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      assertEquals(List.of(insert(1, 10), insert(2, 20)), collect(harness));
      snapshot = transition.snapshot(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            normalizerHarness()) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      VectorSchemaRoot changes =
          RowDataArrowConverter.write(
              List.of(rowOfKind(RowKind.UPDATE_AFTER, 1, 11), rowOfKind(RowKind.DELETE, 2, 0)),
              INPUT,
              allocator,
              true);
      harness.processElement(new StreamRecord<>(new ArrowBatch(changes)));
      assertEquals(
          List.of(
              update(RowKind.UPDATE_BEFORE, 1, 10),
              update(RowKind.UPDATE_AFTER, 1, 11),
              update(RowKind.DELETE, 2, 20)),
          collect(harness));
    }
  }

  /**
   * The append-only Top-N rides the RocksDB LIST store: buffer positions (tie order) survive the
   * restore, so the eviction after restore hits exactly the row Flink would evict.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesTopNTieOrder(StateTransition transition) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            topNHarness()) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Two rows tie on the sort key; arrival order decides who sits at rank 2.
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(9L, 7L), GenericRowData.of(9L, 7L)),
                      TOPN_ROW,
                      allocator))));
      collectDedupless(harness);
      snapshot = transition.snapshot(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            topNHarness()) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // A better row (smaller sort key) evicts rank 2 — the LATER of the tie arrivals.
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(9L, 1L)), TOPN_ROW, allocator))));
      assertEquals(
          List.of(
              List.of(RowKind.DELETE, 9L, 7L), List.of(RowKind.INSERT, 9L, 1L)),
          collectDedupless(harness));
    }
  }

  private static final RowType TOPN_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"p", "s"});

  /**
   * The retracting Top-N keeps its FULL buffer (never truncated to N) on the same list store: a
   * retraction after restore promotes the row that sat beyond rank N, which only works if the
   * whole buffer — not just the visible top — survived the RocksDB round trip.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesRetractingTopNBuffer(StateTransition transition)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            retractingTopNHarness()) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(
                          GenericRowData.of(9L, 1L),
                          GenericRowData.of(9L, 2L),
                          GenericRowData.of(9L, 3L)),
                      TOPN_ROW,
                      allocator))));
      assertEquals(
          List.of(List.of(RowKind.INSERT, 9L, 1L), List.of(RowKind.INSERT, 9L, 2L)),
          collectDedupless(harness));
      snapshot = transition.snapshot(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            retractingTopNHarness()) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // Retracting rank 1 must promote the restored rank-3 row into the top-2.
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(rowOfKind(RowKind.DELETE, 9, 1)), TOPN_ROW, allocator, true))));
      assertEquals(
          List.of(List.of(RowKind.DELETE, 9L, 1L), List.of(RowKind.INSERT, 9L, 3L)),
          collectDedupless(harness));
    }
  }

  private static final RowType UPDATE_FAST_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"p", "k", "s"});

  /**
   * The update-fast Top-N rides its row-keyed RocksDB map shape (PK = the row's unique-key bytes,
   * with the inner rank among sort-key ties persisted alongside the payload): its checkpoint is
   * an incremental RocksDB handle, and after restore a new version of a buffered row key MOVES the
   * row — retracting its old payload — which only works if the persisted row-key identity and the
   * tie order survived the round trip.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesUpdateFastTopNRows(StateTransition transition)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            updateFastTopNHarness()) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Two row keys tie on the sort key; arrival order decides who sits at rank 2.
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(9L, 7L, 5L), GenericRowData.of(9L, 8L, 5L)),
                      UPDATE_FAST_ROW,
                      allocator))));
      collectUpdateFast(harness);
      snapshot = transition.snapshot(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            updateFastTopNHarness()) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // Row key (9, 7)'s next version improves its sort key. The hydrated entry is recognized as
      // an in-place keyed update rather than a fresh insert that would evict the (9, 8) tie row.
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(9L, 7L, 1L)), UPDATE_FAST_ROW, allocator))));
      assertEquals(
          List.of(List.of(RowKind.UPDATE_AFTER, 9L, 7L, 1L)),
          collectUpdateFast(harness));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      updateFastTopNHarness() throws Exception {
    NativeColumnarTopNOperator operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {-1},
            UPDATE_FAST_ROW,
            new int[] {2},
            new int[] {1},
            new int[] {0},
            0L,
            2L,
            false,
            false,
            new int[] {0, 1},
            new int[] {-1, -1},
            false,
            false,
            -1,
            0,
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static List<List<Object>> collectUpdateFast(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, UPDATE_FAST_ROW)) {
            rows.add(List.of(row.getRowKind(), row.getLong(0), row.getLong(1), row.getLong(2)));
          }
        }
      }
    }
    return rows;
  }

  /**
   * The updating join rides two RocksDB tables under one backend (the analog of Flink's two named
   * join states as two column families in one RocksDB): one incremental handle carries both, and a
   * restored joiner's retraction still finds the pre-restore match.
   */
  @ParameterizedTest
  @EnumSource(StateTransition.class)
  void stateTransitionPreservesUpdatingJoinBothSides(StateTransition transition)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<
                Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = joinHarness()) {
      transition.configureSource(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement2(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 100L)), INPUT, allocator))));
      harness.processElement1(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L)), INPUT, allocator))));
      collectJoin(harness);
      snapshot = transition.snapshot(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<
                Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = joinHarness()) {
      transition.configureRestore(harness);
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // Retracting the pre-restore left row must retract its (hydrated) match.
      VectorSchemaRoot retract =
          RowDataArrowConverter.write(
              List.of(rowOfKind(RowKind.DELETE, 1, 10)), INPUT, allocator, true);
      harness.processElement1(new StreamRecord<>(new ArrowBatch(retract)));
      assertEquals(
          List.of(List.of(RowKind.DELETE, 1L, 10L, 1L, 100L)), collectJoin(harness));
    }
  }

  private static org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<
          Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      joinHarness() throws Exception {
    NativeColumnarUpdatingJoinOperator operator =
        new NativeColumnarUpdatingJoinOperator(
            new int[] {0},
            new int[] {0},
            0, // INNER
            INPUT,
            INPUT,
            new int[0],
            new int[0],
            new int[0],
            new long[0],
            new double[0],
            new String[0],
            tech.streamfusion.operator.NativeUdf.Binding.EMPTY,
            new int[] {-1},
            false,
            false,
            false,
            0,
            0,
            0,
            MAX_PARALLELISM);
    return new org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<>(
        operator, batch -> 0, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static final RowType JOIN_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()
          },
          new String[] {"lk", "lv", "rk", "rv"});

  private static List<List<Object>> collectJoin(
      org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<
              Integer, ArrowBatch, ArrowBatch, ArrowBatch>
          harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, JOIN_OUTPUT)) {
            rows.add(
                List.of(
                    row.getRowKind(), row.getLong(0), row.getLong(1), row.getLong(2),
                    row.getLong(3)));
          }
        }
      }
    }
    return rows;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      topNHarness() throws Exception {
    NativeColumnarTopNOperator operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {-1},
            TOPN_ROW,
            new int[] {1},
            new int[] {1},
            new int[] {0},
            0L,
            2L,
            false,
            false,
            null,
            null,
            false,
            false,
            -1,
            0,
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      retractingTopNHarness() throws Exception {
    NativeColumnarTopNOperator operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {-1},
            TOPN_ROW,
            new int[] {1},
            new int[] {1},
            new int[] {0},
            0L,
            2L,
            false,
            true,
            null,
            null,
            false,
            false,
            -1,
            0,
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static List<List<Object>> collectDedupless(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, TOPN_ROW)) {
            rows.add(List.of(row.getRowKind(), row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    return rows;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      normalizerHarness() throws Exception {
    NativeColumnarChangelogNormalizeOperator operator =
        new NativeColumnarChangelogNormalizeOperator(
            new int[] {0}, new int[] {-1}, INPUT, true, false, 0, 0, MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      dedupHarness() throws Exception {
    NativeColumnarKeepLastDeduplicateOperator operator =
        new NativeColumnarKeepLastDeduplicateOperator(
            new int[] {0},
            new int[] {-1},
            2,
            DEDUP_ROW,
            true,
            true,
            true,
            false,
            false,
            false,
            0,
            0,
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static List<List<Object>> collectDedup(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, DEDUP_ROW)) {
            rows.add(List.of(row.getRowKind(), row.getLong(0), row.getLong(1), row.getLong(2)));
          }
        }
      }
    }
    return rows;
  }

  /** OVER input `[k, v, rt]` (rt as BIGINT millis); output appends the running SUM. */
  private static final RowType OVER_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"k", "v", "rt"});
  private static final RowType OVER_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()
          },
          new String[] {"k", "v", "rt", "s"});

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      overHarness(long stateTtlMillis) throws Exception {
    return overHarness(overOperator(new int[] {0}, 0, 0L, false, stateTtlMillis));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      overHarness(NativeOverAggregateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static NativeOverAggregateOperator overOperator(
      int[] kinds, int frameKind, long frameOffset, boolean proctime, long stateTtlMillis) {
    return new NativeOverAggregateOperator(
        2,
        new int[] {1},
        new int[] {0},
        new int[] {0},
        kinds,
        frameKind,
        frameOffset,
        proctime,
        new int[] {-1},
        stateTtlMillis,
        OVER_ROW,
        MAX_PARALLELISM);
  }

  private static ArrowBatch overBatch(BufferAllocator allocator, long v, long rt) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(GenericRowData.of(1L, v, rt)), OVER_ROW, allocator));
  }

  private static List<List<Object>> collectOver(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, OVER_OUTPUT)) {
            rows.add(
                List.of(
                    row.getRowKind(),
                    row.getLong(0),
                    row.getLong(1),
                    row.getLong(2),
                    row.getLong(3)));
          }
        }
      }
    }
    return rows;
  }

  /** Both temporal-join sides `[k, v, rt]` (rt as BIGINT millis); LEFT join, equi key column 0. */
  private static final RowType TEMPORAL_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"k", "v", "rt"});
  private static final RowType TEMPORAL_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType()
          },
          new String[] {"k", "v", "rt", "k0", "v0", "rt0"});

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      temporalHarness(long stateTtlMillis) throws Exception {
    NativeTemporalJoinOperator operator =
        new NativeTemporalJoinOperator(
            new int[] {0},
            new int[] {0},
            2,
            2,
            1, // LEFT
            TEMPORAL_ROW,
            TEMPORAL_ROW,
            EncodedPredicate.NONE,
            new int[] {-1},
            stateTtlMillis,
            MAX_PARALLELISM);
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        operator, batch -> 0, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static List<Object> temporalRow(Long lk, Long lv, Long lrt, Long rk, Long rv, Long rrt) {
    List<Object> values = new ArrayList<>();
    values.add(RowKind.INSERT);
    values.add(lk);
    values.add(lv);
    values.add(lrt);
    values.add(rk);
    values.add(rv);
    values.add(rrt);
    return values;
  }

  private static List<List<Object>> collectTemporal(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
          harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, TEMPORAL_OUTPUT)) {
            List<Object> values = new ArrayList<>();
            values.add(row.getRowKind());
            values.add(row.getLong(0));
            values.add(row.getLong(1));
            values.add(row.getLong(2));
            values.add(row.isNullAt(3) ? null : row.getLong(3));
            values.add(row.isNullAt(4) ? null : row.getLong(4));
            values.add(row.isNullAt(5) ? null : row.getLong(5));
            rows.add(values);
          }
        }
      }
    }
    return rows;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness()
      throws Exception {
    return harness(0);
  }

  private static RocksDBNativeStateBackend backend() {
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
    return new RocksDBNativeStateBackend(
        config, RocksDBNativeStateBackendAllOperatorsTest.class.getClassLoader());
  }

  private static OperatorSubtaskState canonicalSavepoint(
      AbstractStreamOperatorTestHarness<?> harness) throws Exception {
    OperatorSubtaskState savepoint =
        harness
            .snapshotWithLocalState(
                1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState();
    assertInstanceOf(
        org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle.class,
        savepoint.getManagedKeyedState().iterator().next());
    return savepoint;
  }

  private enum StateTransition {
    ROCKSDB_CHECKPOINT,
    MEMORY_TO_ROCKSDB,
    ROCKSDB_TO_MEMORY;

    void configureSource(AbstractStreamOperatorTestHarness<?> harness) {
      if (this != MEMORY_TO_ROCKSDB) {
        harness.setStateBackend(backend());
      }
    }

    void configureRestore(AbstractStreamOperatorTestHarness<?> harness) {
      if (this != ROCKSDB_TO_MEMORY) {
        harness.setStateBackend(backend());
      }
    }

    OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness) throws Exception {
      if (this != ROCKSDB_CHECKPOINT) {
        return canonicalSavepoint(harness);
      }
      OperatorSubtaskState checkpoint = harness.snapshot(1, 1);
      rocksHandle(checkpoint);
      harness.notifyOfCompletedCheckpoint(1);
      return checkpoint;
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      long stateTtlMillis) throws Exception {
    return harness(groupAggregateOperator(stateTtlMillis));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      NativeColumnarGroupAggregateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static NativeColumnarGroupAggregateOperator groupAggregateOperator(
      long stateTtlMillis) {
    return new NativeColumnarGroupAggregateOperator(
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
        stateTtlMillis,
        new int[] {-1},
        MAX_PARALLELISM);
  }

  /** Both window-join sides `[k, v, window_start, window_end]`; the pre-attached window is [0, 1000). */
  private static final RowType WINDOW_JOIN_ROW =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new LocalZonedTimestampType(3)
          },
          new String[] {"k", "v", "window_start", "window_end"});

  private static final RowType WINDOW_JOIN_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new LocalZonedTimestampType(3),
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new LocalZonedTimestampType(3)
          },
          new String[] {"lk", "lv", "ls", "le", "rk", "rv", "rs", "re"});

  private static NativeWindowJoinOperator windowJoinOperator() {
    return new NativeWindowJoinOperator(
        new int[] {0}, new int[] {0}, 2, 3, 2, 3, 0, WINDOW_JOIN_ROW, WINDOW_JOIN_ROW,
        EncodedPredicate.NONE, false, 1000, 1000, false, new int[] {-1}, MAX_PARALLELISM);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      windowJoinHarness(NativeWindowJoinOperator operator) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        operator, batch -> 0, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static ArrowBatch windowJoinBatch(BufferAllocator allocator, long key, long value) {
    GenericRowData row = new GenericRowData(4);
    row.setField(0, key);
    row.setField(1, value);
    row.setField(2, TimestampData.fromEpochMillis(0));
    row.setField(3, TimestampData.fromEpochMillis(1000));
    return new ArrowBatch(RowDataArrowConverter.write(List.of(row), WINDOW_JOIN_ROW, allocator));
  }

  private static List<List<Long>> collectWindowJoinPairs(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, WINDOW_JOIN_OUTPUT)) {
            rows.add(List.of(row.getLong(0), row.getLong(1), row.getLong(4), row.getLong(5)));
          }
        }
      }
    }
    return rows;
  }

  /** Inner interval join `a.rt BETWEEN b.rt - 1000 AND b.rt + 1000` over {@link #TEMPORAL_ROW}. */
  private static NativeIntervalJoinOperator intervalJoinOperator() {
    return new NativeIntervalJoinOperator(
        new int[] {0}, new int[] {0}, 2, 2, -1000L, 1000L, 0, TEMPORAL_ROW, TEMPORAL_ROW,
        EncodedPredicate.NONE, false, new int[] {-1}, MAX_PARALLELISM);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      intervalJoinHarness(NativeIntervalJoinOperator operator) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        operator, batch -> 0, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  /** Window rank rows `[v, window_start, window_end]`; top-2 by v descending with rank numbers. */
  private static final RowType RANK_ROW =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new LocalZonedTimestampType(3), new LocalZonedTimestampType(3)
          },
          new String[] {"v", "window_start", "window_end"});

  private static final RowType RANK_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new TimestampType(3), new TimestampType(3), new BigIntType()
          },
          new String[] {"v", "window_start", "window_end", "w0$o0"});

  private static NativeColumnarWindowRankOperator windowRankOperator() {
    return new NativeColumnarWindowRankOperator(
        1, 2, new int[0], new int[0], new int[] {0}, new int[] {0}, new int[] {0}, 2, true,
        "UTC", false, 0, 0, false, RANK_ROW, MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      windowRankHarness(NativeColumnarWindowRankOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static RowData rankRow(long value) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(0));
    row.setField(2, TimestampData.fromEpochMillis(1000));
    return row;
  }

  private static ArrowBatch rankBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), RANK_ROW, allocator));
  }

  private static List<List<Long>> collectRanked(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, RANK_OUTPUT)) {
            rows.add(List.of(row.getLong(0), row.getLong(3)));
          }
        }
      }
    }
    return rows;
  }

  /** Session rows `[value, rt]` under a 500ms gap; output `[total, window_start, window_end]`. */
  private static final RowType SESSION_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"value", "rt"});

  private static final RowType SESSION_OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new TimestampType(3), new TimestampType(3)},
          new String[] {"total", "window_start", "window_end"});

  private static NativeColumnarSessionWindowAggregateOperator sessionOperator() {
    return new NativeColumnarSessionWindowAggregateOperator(
        500, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0}, "UTC",
        SESSION_OUTPUT, false, new int[0], MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      sessionHarness(NativeColumnarSessionWindowAggregateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static ArrowBatch sessionBatch(BufferAllocator allocator, long value, long eventTime) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(eventTime));
    return new ArrowBatch(RowDataArrowConverter.write(List.of(row), SESSION_ROW, allocator));
  }

  private static List<List<Long>> collectSessions(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, SESSION_OUTPUT)) {
            rows.add(
                List.of(
                    row.getLong(0),
                    row.getTimestamp(1, 3).getMillisecond(),
                    row.getTimestamp(2, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }

  /** Keep-first over {@link #DEDUP_ROW}: partition key column 0, rowtime column 2 (BIGINT millis). */
  private static NativeColumnarDeduplicateOperator keepFirstOperator() {
    return new NativeColumnarDeduplicateOperator(
        new int[] {0}, new int[] {-1}, 2, DEDUP_ROW, 0, MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      keepFirstHarness(NativeColumnarDeduplicateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  /** Buffers three keys' rows and fires watermark 500, emitting key 1's first row `(1, 20)`. */
  private static void keepFirstPhaseOne(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      BufferAllocator allocator)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    List.of(
                        GenericRowData.of(1L, 30L, 2000L),
                        GenericRowData.of(2L, 40L, 1000L),
                        GenericRowData.of(1L, 20L, 0L)),
                    DEDUP_ROW,
                    allocator))));
    harness.processWatermark(new Watermark(500));
  }

  /**
   * The remaining input: a row for the already-fired key 1 (suppressed by its marker), a key-3 row
   * below the watermark 500 (dropped as late), a live key-3 candidate, and the closing watermark.
   */
  private static List<List<Long>> keepFirstPhaseTwo(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      BufferAllocator allocator)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    List.of(
                        GenericRowData.of(1L, 99L, 1500L),
                        GenericRowData.of(3L, 7L, 300L),
                        GenericRowData.of(3L, 8L, 1200L)),
                    DEDUP_ROW,
                    allocator))));
    harness.processWatermark(new Watermark(3000));
    return collectKeepFirst(harness);
  }

  private static List<List<Long>> collectKeepFirst(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, DEDUP_ROW)) {
            rows.add(List.of(row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    rows.sort(java.util.Comparator.comparingLong(row -> row.get(0)));
    return rows;
  }

  /** Sorter rows `[v, rt]`; ordered by rt (column 1). The sorter owns one canonical empty key. */
  private static final RowType SORT_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"v", "rt"});

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      sorterHarness(NativeColumnarTemporalSortOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(operator, batch -> 0, Types.INT, 1, 1, 0);
  }

  private static RowData sortRow(long value, long rtMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch sortBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SORT_ROW, allocator));
  }

  private static List<List<Long>> collectSorted(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, SORT_ROW)) {
            rows.add(List.of(row.getLong(0), row.getTimestamp(1, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }

  private static IncrementalRemoteKeyedStateHandle rocksHandle(OperatorSubtaskState state) {
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
