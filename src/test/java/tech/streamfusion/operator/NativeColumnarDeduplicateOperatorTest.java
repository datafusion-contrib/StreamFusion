package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import tech.streamfusion.planner.FlinkKeyGroupUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The keep-first deduplicate operator emits, per partition key, the minimum-rowtime row once the
 * watermark reaches that rowtime — not the first to arrive. After a key emits, later rows for it are
 * ignored, and a row arriving with a rowtime already below the watermark is dropped as late.
 */
@ExtendWith(CoalescingOff.class)
class NativeColumnarDeduplicateOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  // [k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)]; partition key column 0, rowtime column 2.
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "v", "rt"});

  @Test
  void emitsMinimumRowtimeRowPerKeyOnWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            keepFirstHarness(1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // key 1: rows at rt 2000, 0, 800 -> min-rowtime row is (v=20, rt=0). key 2: single (v=40, rt=1000).
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 30, 2000), row(2, 40, 1000), row(1, 20, 0), row(1, 25, 800))));

      // Watermark 1000 releases both keys' first rows (rt 0 and 1000 are <= 1000).
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(emitted(1, 20), emitted(2, 40)), collect(harness));

      // A later row for the already-emitted key 1 is ignored; a row for key 3 below the watermark is
      // dropped as late; key 3's in-time row becomes its candidate.
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 99, 1500), row(3, 7, 300), row(3, 8, 1200))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(emitted(3, 8)), collect(harness));
    }
  }

  @Test
  void eagerDeduplicationRawKeyedStateRescalesByFlinkKeyGroup() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            eagerHarness(1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(keys[0], 10, 1000), row(keys[1], 20, 1000))));
      snapshot = before.snapshot(1L, 1L);
      assertEquals(
          List.of(List.of("+I", keys[0], 10L), List.of("+I", keys[1], 20L)),
          collectChanges(before));
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            eagerHarness(2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            eagerHarness(2, 1)) {
      restored0.setup(new ArrowBatchSerializer());
      restored1.setup(new ArrowBatchSerializer());
      restored0.initializeState(
          AbstractStreamOperatorTestHarness.repartitionOperatorState(
              snapshot, MAX_PARALLELISM, 1, 2, 0));
      restored1.initializeState(
          AbstractStreamOperatorTestHarness.repartitionOperatorState(
              snapshot, MAX_PARALLELISM, 1, 2, 1));
      restored0.open();
      restored1.open();

      for (long key : keys) {
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> destination =
            destinationForKey(key) == 0 ? restored0 : restored1;
        destination.processElement(
            new StreamRecord<>(
                batch(
                    allocator,
                    destinationForKey(key),
                    row(key, key == keys[0] ? 11 : 21, 2000))));
      }
      List<List<Object>> actual = new ArrayList<>();
      actual.addAll(collectChanges(restored0));
      actual.addAll(collectChanges(restored1));
      actual.sort(Comparator.comparing(row -> (Long) row.get(1)));
      List<List<Object>> expected =
          new ArrayList<>(
              List.of(
                  List.of("-U", keys[0], 10L),
                  List.of("+U", keys[0], 11L),
                  List.of("-U", keys[1], 20L),
                  List.of("+U", keys[1], 21L)));
      expected.sort(Comparator.comparing(row -> (Long) row.get(1)));
      assertEquals(expected, actual);
    }
  }

  @Test
  void keepLastCoalescesPhysicalBatchesAtTheLogicalBoundary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, true, 4)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10, 0), row(1, 20, 1))));
      assertEquals(List.of(), collectChanges(harness));
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(2, 5, 0), row(1, 30, 2))));
      // The rowtime flush emits every kept row's transition (Flink's mini-batch full changelog),
      // grouped per key in staging order — not one net change per key.
      assertEquals(
          List.of(
              List.of("+I", 1L, 10L),
              List.of("-U", 1L, 10L),
              List.of("+U", 1L, 20L),
              List.of("-U", 1L, 20L),
              List.of("+U", 1L, 30L),
              List.of("+I", 2L, 5L)),
          collectChanges(harness));
    }
  }

  @Test
  void keepLastCompactChangesEmitsOnlyTheBundleEndpoint() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, true, true, 4, true, true, true, false, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10, 0), row(1, 20, 1))));
      assertEquals(List.of(), collectChanges(harness));
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(2, 5, 0), row(1, 30, 2))));
      // Compact-changes nets each key's bundle to one transition — key 1's improving chain
      // collapses to a single +I of its endpoint (contrast the full-changelog flush above).
      assertEquals(
          List.of(List.of("+I", 1L, 30L), List.of("+I", 2L, 5L)), collectChanges(harness));
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 40, 3), row(1, 50, 4), row(2, 6, 0), row(2, 7, 1))));
      assertEquals(
          List.of(
              List.of("-U", 1L, 30L),
              List.of("+U", 1L, 50L),
              List.of("-U", 2L, 5L),
              List.of("+U", 2L, 7L)),
          collectChanges(harness));
    }
  }

  @Test
  void insertInsensitiveKeepLastEmitsBareUpdateAfterKinds() throws Exception {
    // generateUpdateBefore=false models an only-update-after consumer; with generateInsert=false
    // as well (table.exec.deduplicate.insert-update-after-sensitive-enabled off), Flink's proctime
    // helper takes its stateless branch: EVERY row emits a bare +U — the fresh key's first row,
    // the identical duplicate the insert-sensitive mode would suppress, and each replacement.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, false, false, 0, false, false, false, false, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 0))));
      assertEquals(List.of(List.of("+U", 1L, 10L)), collectChanges(harness));
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 0), row(1, 20, 0))));
      assertEquals(
          List.of(List.of("+U", 1L, 10L), List.of("+U", 1L, 20L)), collectChanges(harness));
    }
  }

  @Test
  void keepFirstMiniBatchChainsImprovingRowtimesLikeFlinksBundledFunction() throws Exception {
    // Rowtime keep-first under mini-batch is Flink's bundled retracting function: a strictly
    // smaller rowtime displaces with -U/+U (a tie would keep the incumbent), emitted as the full
    // kept chain at the logical boundary — the keep-last flush with the comparator flipped.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, true, false, 4, true, true, true, true, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10, 300), row(1, 20, 200))));
      assertEquals(List.of(), collectChanges(harness));
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(2, 5, 100), row(1, 30, 100))));
      assertEquals(
          List.of(
              List.of("+I", 1L, 10L),
              List.of("-U", 1L, 10L),
              List.of("+U", 1L, 20L),
              List.of("-U", 1L, 20L),
              List.of("+U", 1L, 30L),
              List.of("+I", 2L, 5L)),
          collectChanges(harness));
    }
  }

  @Test
  void keepLastFlushesAnIncompleteLogicalBatchBeforeWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, true, 4)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10, 0), row(1, 20, 1))));
      assertEquals(List.of(), collectChanges(harness));

      harness.processWatermark(new Watermark(1));
      assertEquals(
          List.of(List.of("+I", 1L, 10L), List.of("-U", 1L, 10L), List.of("+U", 1L, 20L)),
          collectChanges(harness));
    }
  }

  @Test
  void ttlExpiresAnIdleKeyIntoAFreshInsert() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            ttlHarness(1000)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 0))));
      assertEquals(List.of(List.of("+I", 1L, 10L)), collectChanges(harness));
      // ts 5000 + ttl 1000 <= 6000: expired exactly at the boundary — a fresh +I, not -U/+U.
      harness.setProcessingTime(6000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 20, 0))));
      assertEquals(List.of(List.of("+I", 1L, 20L)), collectChanges(harness));
    }
  }

  @Test
  void ttlRefreshesOnEveryWrite() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            ttlHarness(1000)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 0))));
      collectChanges(harness);
      harness.setProcessingTime(5900);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 20, 0))));
      collectChanges(harness);
      // The original write is long past its ttl, but the write at 5900 refreshed the key.
      harness.setProcessingTime(6800);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 30, 0))));
      assertEquals(
          List.of(List.of("-U", 1L, 20L), List.of("+U", 1L, 30L)), collectChanges(harness));
    }
  }

  @Test
  void ttlTimestampsSurviveSnapshotRestore() throws Exception {
    // Timestamps are absolute: expiry after a restore is timed from the original write, so a
    // restore inside the retention keeps the key alive only until write-time + ttl.
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            ttlHarness(1000)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 0))));
      snapshot = harness.snapshot(1L, 1L);
      collectChanges(harness);
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            ttlHarness(1000)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(5999);
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 20, 0))));
      assertEquals(
          List.of(List.of("-U", 1L, 10L), List.of("+U", 1L, 20L)), collectChanges(restored));
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            ttlHarness(1000)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(6000);
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 20, 0))));
      assertEquals(List.of(List.of("+I", 1L, 20L)), collectChanges(restored));
    }
  }

  @Test
  void ttlExpiredEmittedMarkerLetsTheKeyFireAgain() throws Exception {
    // The watermark-buffered keep-first TTLs only its emitted markers (Flink's
    // alreadyEmittedState — the buffered candidate mirrors the deliberately un-TTL'd timer
    // state): the marker is written once when the key fires and never refreshed by later dropped
    // rows, so an expired marker lets the key buffer a new candidate and emit a second first row.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keepFirstHarness(1, 0, 1000)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 1000))));
      harness.processWatermark(new Watermark(1000)); // fires — the marker is stamped at 5000
      assertEquals(List.of(emitted(1, 10)), collect(harness));
      // A later row while the marker is alive is dropped, and the read does not refresh it.
      harness.setProcessingTime(5500);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 20, 1500))));
      harness.processWatermark(new Watermark(2000));
      assertEquals(List.of(), collect(harness));
      // 5000 + 1000 <= 6000: the marker expired despite the probe at 5500 — the key re-buffers
      // and fires a second +I.
      harness.setProcessingTime(6000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 30, 2500))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(emitted(1, 30)), collect(harness));
    }
  }

  @Test
  void lateRowsIncrementTheDroppedCounterWithoutOutput() throws Exception {
    // Flink's RowTimeDeduplicateKeepFirstRowFunction counts every row with rowtime strictly
    // below the current watermark under numLateRecordsDropped; a row exactly at the watermark is
    // not late, and a live row for an already-emitted key is an ignore, not a late drop.
    NativeColumnarDeduplicateOperator operator = keepFirstOperator(0);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keepFirstHarness(operator, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10, 1000))));
      harness.processWatermark(new Watermark(2000));
      assertEquals(List.of(emitted(1, 10)), collect(harness));
      assertEquals(0, operator.numLateRecordsDropped().getCount());

      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(2, 7, 1500), row(3, 8, 2000), row(1, 9, 2500))));
      assertEquals(1, operator.numLateRecordsDropped().getCount());
      harness.processWatermark(new Watermark(3000));
      // The late row for key 2 never emits; key 3's at-watermark row fires normally.
      assertEquals(List.of(emitted(3, 8)), collect(harness));
    }
  }

  @Test
  void bufferedDeduplicationRawKeyedStateRescalesByFlinkKeyGroup() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            keepFirstHarness(1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(keys[0], 10, 1000), row(keys[1], 20, 2000))));
      snapshot = before.snapshot(1L, 1L);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            keepFirstHarness(2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            keepFirstHarness(2, 1)) {
      restored0.setup(new ArrowBatchSerializer());
      restored1.setup(new ArrowBatchSerializer());
      restored0.initializeState(
          AbstractStreamOperatorTestHarness.repartitionOperatorState(
              snapshot, MAX_PARALLELISM, 1, 2, 0));
      restored1.initializeState(
          AbstractStreamOperatorTestHarness.repartitionOperatorState(
              snapshot, MAX_PARALLELISM, 1, 2, 1));
      restored0.open();
      restored1.open();
      restored0.processWatermark(new Watermark(3000));
      restored1.processWatermark(new Watermark(3000));
      List<List<Long>> actual = new ArrayList<>();
      actual.addAll(collect(restored0));
      actual.addAll(collect(restored1));
      actual.sort(Comparator.comparingLong(row -> row.get(0)));
      List<List<Long>> expected = new ArrayList<>(List.of(emitted(keys[0], 10), emitted(keys[1], 20)));
      expected.sort(Comparator.comparingLong(row -> row.get(0)));
      assertEquals(expected, actual);
    }
  }

  private static RowData row(long k, long v, long rtMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, k);
    row.setField(1, v);
    row.setField(2, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator));
  }

  private static ArrowBatch batch(BufferAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator), destination);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      eagerHarness(int parallelism, int subtask) throws Exception {
    return eagerHarness(parallelism, subtask, false, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      eagerHarness(int parallelism, int subtask, boolean miniBatch, long miniBatchSize)
          throws Exception {
    return eagerHarness(
        parallelism, subtask, miniBatch, false, miniBatchSize, true, true, true, false, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      eagerHarness(
          int parallelism,
          int subtask,
          boolean miniBatch,
          boolean compactChanges,
          long miniBatchSize,
          boolean generateUpdateBefore,
          boolean generateInsert,
          boolean rowtimeOrdered,
          boolean keepFirst,
          long stateTtlMillis)
          throws Exception {
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarKeepLastDeduplicateOperator(
            new int[] {0},
            new int[] {-1},
            2,
            SCHEMA,
            generateUpdateBefore,
            generateInsert,
            rowtimeOrdered,
            keepFirst,
            miniBatch,
            compactChanges,
            miniBatchSize,
            stateTtlMillis,
            MAX_PARALLELISM),
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  /** Proctime keep-last (arrival order), so the TTL tests replace unconditionally. */
  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> ttlHarness(
      long stateTtlMillis) throws Exception {
    return eagerHarness(1, 0, false, false, 0, true, true, false, false, stateTtlMillis);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      keepFirstHarness(int parallelism, int subtask) throws Exception {
    return keepFirstHarness(parallelism, subtask, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      keepFirstHarness(int parallelism, int subtask, long stateTtlMillis) throws Exception {
    return keepFirstHarness(keepFirstOperator(stateTtlMillis), parallelism, subtask);
  }

  private static NativeColumnarDeduplicateOperator keepFirstOperator(long stateTtlMillis) {
    return new NativeColumnarDeduplicateOperator(
        new int[] {0},
        new int[] {-1},
        2,
        RowType.of(new BigIntType(), new BigIntType(), new BigIntType()),
        stateTtlMillis,
        MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      keepFirstHarness(NativeColumnarDeduplicateOperator operator, int parallelism, int subtask)
          throws Exception {
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static List<Long> emitted(long k, long v) {
    return List.of(k, v);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(r.getLong(0), r.getLong(1)));
          }
        }
      }
    }
    rows.sort(Comparator.comparingLong(r -> r.get(0)));
    return rows;
  }

  private static List<List<Object>> collectChanges(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(row.getRowKind().shortString(), row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    return rows;
  }

  private static long[] keysForBothSubtasks() {
    long[] keys = new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
    for (long candidate = 0;
        candidate < 10_000 && (keys[0] == Long.MIN_VALUE || keys[1] == Long.MIN_VALUE);
        candidate++) {
      int subtask = destinationForKey(candidate);
      if (keys[subtask] == Long.MIN_VALUE) {
        keys[subtask] = candidate;
      }
    }
    if (keys[0] == Long.MIN_VALUE || keys[1] == Long.MIN_VALUE) {
      throw new AssertionError("did not find one key for each rescaled subtask");
    }
    return keys;
  }

  private static int destinationForKey(long key) {
    int keyGroup =
        KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
            new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                    RowType.of(new BigIntType()))
                .toBinaryRow(GenericRowData.of(key))
                .hashCode(),
            MAX_PARALLELISM);
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keyGroup);
  }
}
