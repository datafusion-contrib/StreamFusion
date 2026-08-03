package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.EncodedPredicate;
import tech.streamfusion.operator.NativeColumnarChangelogNormalizeOperator;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.NativeColumnarKeepLastDeduplicateOperator;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.operator.NativeColumnarUpdatingJoinOperator;
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.operator.NativeTemporalJoinOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.CoalescingOff;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Native operators on the Paimon state backend: state lives in a local Paimon table, snapshots go
 * through the keyed-state backend as {@link IncrementalRemoteKeyedStateHandle}s (not raw keyed
 * state), a completed checkpoint's files are referenced by placeholders instead of re-uploaded
 * (incremental), and a fresh operator restored from the handle continues the changelog exactly.
 *
 * <p>Lives in the compactor module because the backend fails closed without the Java compactor
 * on the classpath: every run here is the production shape — state tables carry deletion
 * vectors and barriers compact synchronously.
 */
@ExtendWith(CoalescingOff.class)
class PaimonStateBackendOperatorTest {

  private static final int MAX_PARALLELISM = 128;

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

  /**
   * A TTL'd group aggregate rides the Paimon route too (its snapshot is an incremental Paimon
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
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      assertEquals(List.of(insert(1, 10)), collect(harness));
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot); // the TTL'd aggregate must resolve to the Paimon route
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(1000)) {
      harness.setStateBackend(new PaimonStateBackend());
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
   * A retention-bounded event-time OVER rides the Paimon route: the per-key cleanup deadline
   * persists absolutely in the deadlines table, so after restore the key folds fresh at exactly
   * the writer's deadline. The deadline shapes deliberately register no retention with the
   * backend ({@code resolvePaimon} without a TTL) — a deferred or re-armed deadline is not a
   * truthful per-row clock, so every maintenance session opens WITHOUT record-level expiry
   * options ({@link #recordLevelExpireOptionsPadTheRetention} pins the zero-retention mapping,
   * and {@code JavaPaimonStateCompactorTtlTest.sessionWithoutOptionsNeverDropsRows} that such a
   * session never drops rows); physical cleanup is the operator's own staged tombstones.
   */
  @Test
  void overAggregateRetentionExpiresAcrossCheckpointAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(2000)) {
      harness.setStateBackend(new PaimonStateBackend());
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
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot); // the retention-bounded OVER must resolve to the Paimon route
      harness.notifyOfCompletedCheckpoint(1);
    }

    // One ms inside the restored (absolute) deadline the fold continues from the table...
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            overHarness(2000)) {
      harness.setStateBackend(new PaimonStateBackend());
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
      harness.setStateBackend(new PaimonStateBackend());
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
   * A retention-bounded temporal join rides the Paimon route with the same absolute-deadline
   * semantics: past the restored deadline the key's whole state — both sides — is gone, so the
   * probe null-pads exactly as if no version ever existed. See the OVER test above for why the
   * maintenance session gets no record-level expiry options.
   */
  @Test
  void temporalJoinRetentionExpiresAcrossCheckpointAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = temporalHarness(2000)) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // The build version at 5000 arms the key's cleanup deadline at 5000 + 1.5x2000 = 8000.
      harness.setProcessingTime(5000);
      harness.processElement2(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1L, 10L, 100L)), TEMPORAL_ROW, allocator, true))));
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot); // the retention-bounded join must resolve to the Paimon route
      harness.notifyOfCompletedCheckpoint(1);
    }

    // One ms inside the restored (absolute) deadline the probe still joins the version...
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = temporalHarness(2000)) {
      harness.setStateBackend(new PaimonStateBackend());
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
      harness.setStateBackend(new PaimonStateBackend());
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
   * The retention handed to a maintenance session as record-level-expire options: nothing
   * without TTL, and padded seconds with it — {@code ceil(ttl/1000) + 1}, so Paimon's
   * whole-second truncation (of both the clock and the ts column) can never physically drop a
   * row before its logical {@code ts + ttl} expiry.
   */
  @Test
  void recordLevelExpireOptionsPadTheRetention() {
    assertEquals(Map.of(), PaimonSnapshotStrategy.recordLevelExpireOptions(0));
    assertEquals(
        Map.of("record-level.expire-time", "2s", "record-level.time-field", "ts"),
        PaimonSnapshotStrategy.recordLevelExpireOptions(1));
    assertEquals(
        Map.of("record-level.expire-time", "2s", "record-level.time-field", "ts"),
        PaimonSnapshotStrategy.recordLevelExpireOptions(1000));
    assertEquals(
        Map.of("record-level.expire-time", "3s", "record-level.time-field", "ts"),
        PaimonSnapshotStrategy.recordLevelExpireOptions(1001));
    assertEquals(
        Map.of("record-level.expire-time", "3601s", "record-level.time-field", "ts"),
        PaimonSnapshotStrategy.recordLevelExpireOptions(3_600_000));
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

  /**
   * The keep-last deduplicator rides the same backend: its checkpoint is an incremental Paimon
   * handle, and a restored operator's retraction carries the payload persisted before the restore.
   */
  @Test
  void dedupCheckpointsAndRestores() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            dedupHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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

      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot); // the dedup checkpoint travels as an incremental handle, not raw state
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            dedupHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
  @Test
  void normalizerCheckpointsAndRestores() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            normalizerHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      assertEquals(List.of(insert(1, 10), insert(2, 20)), collect(harness));
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot);
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            normalizerHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
   * The append-only Top-N rides the Paimon LIST store: buffer positions (tie order) survive the
   * restore, so the eviction after restore hits exactly the row Flink would evict.
   */
  @Test
  void topNCheckpointsAndRestoresTieOrder() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            topNHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot);
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            topNHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
   * whole buffer — not just the visible top — survived the Paimon round trip.
   */
  @Test
  void retractingTopNPromotesFromBeyondNAfterRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            retractingTopNHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot);
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            retractingTopNHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
          List.of(List.of(RowKind.INSERT, 9L, 3L), List.of(RowKind.DELETE, 9L, 1L)),
          collectDedupless(harness));
    }
  }

  private static final RowType UPDATE_FAST_ROW =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"p", "k", "s"});

  /**
   * The update-fast Top-N rides its row-keyed Paimon map shape (PK = the row's unique-key bytes,
   * with the inner rank among sort-key ties persisted alongside the payload): its checkpoint is
   * an incremental Paimon handle, and after restore a new version of a buffered row key MOVES the
   * row — retracting its old payload — which only works if the persisted row-key identity and the
   * tie order survived the round trip.
   */
  @Test
  void updateFastTopNCheckpointsAndRestoresRowKeyedMoves() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            updateFastTopNHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot);
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            updateFastTopNHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();

      // Row key (9, 7)'s next version improves its sort key: the hydrated entry moves, retracting
      // the old payload — a fresh insert would instead have evicted the (9, 8) tie row.
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(9L, 7L, 1L)), UPDATE_FAST_ROW, allocator))));
      assertEquals(
          List.of(
              List.of(RowKind.INSERT, 9L, 7L, 1L), List.of(RowKind.DELETE, 9L, 7L, 5L)),
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
   * The updating join rides two Paimon tables under one backend (the analog of Flink's two named
   * join states as two column families in one RocksDB): one incremental handle carries both, and a
   * restored joiner's retraction still finds the pre-restore match.
   */
  @Test
  void updatingJoinCheckpointsBothSidesAndRestores() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<
                Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = joinHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
      snapshot = harness.snapshot(1, 1);
      paimonHandle(snapshot); // one incremental handle covers both side tables
      harness.notifyOfCompletedCheckpoint(1);
    }

    try (BufferAllocator allocator = new RootAllocator();
        org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<
                Integer, ArrowBatch, ArrowBatch, ArrowBatch>
            harness = joinHarness()) {
      harness.setStateBackend(new PaimonStateBackend());
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
    NativeOverAggregateOperator operator =
        new NativeOverAggregateOperator(
            2,
            new int[] {1},
            new int[] {0},
            new int[] {0},
            new int[] {0},
            0,
            0L,
            false,
            new int[] {-1},
            stateTtlMillis,
            OVER_ROW,
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
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

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      long stateTtlMillis) throws Exception {
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
            stateTtlMillis,
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
