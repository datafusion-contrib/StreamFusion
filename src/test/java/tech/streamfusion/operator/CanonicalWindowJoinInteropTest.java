package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.window.stream.WindowTimeDomain;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.*;

class CanonicalWindowJoinInteropTest {
  static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType());
  static final SavepointType CANONICAL = SavepointType.savepoint(SavepointFormatType.CANONICAL);
  static final long BASE = Instant.parse("2025-03-09T06:59:58Z").toEpochMilli();

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    var values = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL"))
      for (boolean stockStore : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (String zone : List.of("UTC", "Asia/Kolkata", "America/New_York"))
              values.add(Arguments.of(kind, stockStore, nativeStore, keyless, zone));
    return Boolean.getBoolean("streamfusion.test.windowJoinSmoke")
        ? values.stream().limit(1)
        : values.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void pendingListsAndTimersSurviveImmediateReexportNativeCheckpointAndContinuedInput(
      String kind, boolean stockStore, boolean nativeStore, boolean keyless, String zone)
      throws Exception {
    var plan = new Plan(kind, keyless, zone);
    OperatorSubtaskState saved;
    try (var source = plan.stock(stockStore)) {
      source.open();
      plan.prefix(source);
      plan.watermarks(source, plan.deadline(plan.local + 1000));
      assertFalse(stockOutput(source).isEmpty());
      saved = snapshot(source);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState checkpoint;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(saved);
      nativeOp.open();
      assertEquals(
          allocated,
          NativeAllocator.SHARED.getAllocatedMemory(),
          "Import retained JVM Arrow buffers");
      assertEquals(
          nativeStore, ((NativeWindowJoinOperator) nativeOp.getOperator()).directRocksDBState());
      saved = snapshot(nativeOp);
      try (var stock = plan.stock(stockStore)) {
        stock.initializeState(saved);
        stock.open();
        plan.extra(stock);
        plan.extraNative(nativeOp);
        nativeOp.prepareSnapshotPreBarrier(2);
        checkpoint = nativeOp.snapshot(2, 0);
        assertTrue(
            checkpoint.getManagedKeyedState().stream()
                .noneMatch(
                    handle ->
                        handle
                            instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
        try (var resumed = plan.nativeHarness(nativeStore)) {
          resumed.initializeState(checkpoint);
          resumed.open();
          saved = snapshot(resumed);
          try (var again = plan.stock(!stockStore)) {
            again.initializeState(saved);
            again.open();
            verifyStockMetadata(again);
            for (long end : new long[] {plan.local + 1000, plan.local + 2000, plan.local + 4000}) {
              long mark = plan.deadline(end);
              plan.watermarks(stock, mark);
              plan.watermarks(again, mark);
              resumed.processWatermark1(new Watermark(mark));
              resumed.processWatermark2(new Watermark(mark));
              var expected = stockOutput(stock);
              assertEquals(expected, stockOutput(again));
              assertEquals(expected, nativeOutput(resumed));
            }
            var late = plan.row(1L, 999L, plan.local + 2000);
            stock.processElement1(new StreamRecord<>(late));
            again.processElement1(new StreamRecord<>(late));
            push(resumed, true, late);
            plan.watermarks(stock, Long.MAX_VALUE);
            plan.watermarks(again, Long.MAX_VALUE);
            resumed.processWatermark1(new Watermark(Long.MAX_VALUE));
            resumed.processWatermark2(new Watermark(Long.MAX_VALUE));
            assertEquals(stockOutput(stock), stockOutput(again));
            assertTrue(nativeOutput(resumed).isEmpty());
          }
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void nativeIngressExportsIntoTheReleasedStockOperator(
      String kind, boolean stockStore, boolean nativeStore, boolean keyless, String zone)
      throws Exception {
    var plan = new Plan(kind, keyless, zone);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      reference.open();
      nativeOp.open();
      plan.prefix(reference);
      plan.prefixNative(nativeOp);
      try (var restored = plan.stock(!stockStore)) {
        restored.initializeState(snapshot(nativeOp));
        restored.open();
        verifyStockMetadata(restored);
        plan.extra(reference);
        plan.extra(restored);
        plan.extraNative(nativeOp);
        for (long end : new long[] {plan.local + 1000, plan.local + 2000, plan.local + 4000}) {
          long mark = plan.deadline(end);
          plan.watermarks(reference, mark);
          plan.watermarks(restored, mark);
          nativeOp.processWatermark1(new Watermark(mark));
          nativeOp.processWatermark2(new Watermark(mark));
          var expected = stockOutput(reference);
          assertEquals(expected, stockOutput(restored));
          assertEquals(expected, nativeOutput(nativeOp));
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest(name = "kind={0},stockRocks={1},nativeRocks={2},keyless={3},zone={4}")
  @MethodSource("cases")
  void pendingRowsAndTimersFinishTwoPinnedViewsAfterLiveWritesAndOwnerClose(
      String kind, boolean stockStore, boolean nativeStore, boolean keyless, String zone)
      throws Exception {
    var plan = new Plan(kind, keyless, zone);
    var captured = new org.apache.flink.streaming.api.operators.OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    try (var reference = plan.stock(stockStore);
        var source = plan.nativeHarness(nativeStore)) {
      reference.open();
      plan.prefix(reference);
      plan.watermarks(reference, plan.deadline(plan.local + 1000));
      stockOutput(reference);
      expected[0] = snapshot(reference);
      source.initializeState(expected[0]);
      source.open();
      source.processWatermark1(new Watermark(plan.deadline(plan.local + 1000)));
      source.processWatermark2(new Watermark(plan.deadline(plan.local + 1000)));
      var owner = (NativeWindowJoinOperator) source.getOperator();
      assertEquals(nativeStore, owner.directRocksDBState());
      try {
        captured[0] = CanonicalDistinctWindowAsyncTest.capture(source, 2);
        plan.extra(reference);
        plan.extraNative(source);
        expected[1] = snapshot(reference);
        captured[1] = CanonicalDistinctWindowAsyncTest.capture(source, 3);
        plan.extraNative(source);
        source.prepareSnapshotPreBarrier(4);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var stock = plan.stock(stockStore);
              var returned = plan.stock(!stockStore)) {
            stock.initializeState(expected[stage]);
            returned.initializeState(saved);
            stock.open();
            returned.open();
            plan.extra(stock);
            plan.extra(returned);
            int emitted = 0;
            for (long end : new long[] {plan.local + 2000, plan.local + 4000}) {
              plan.watermarks(stock, plan.deadline(end));
              plan.watermarks(returned, plan.deadline(end));
              var rows = stockOutput(stock);
              emitted += rows.size();
              assertEquals(rows, stockOutput(returned));
            }
            assertTrue(emitted > 0);
          } finally {
            saved.discardState();
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
        CanonicalDistinctWindowAsyncTest.discardOutput(source);
      }
    } finally {
      for (var state : expected) if (state != null) state.discardState();
    }
  }

  static Stream<Arguments> shapes() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
            cases.add(Arguments.of(kind, stock, nativeStore, shape));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("shapes")
  void emptyAndOverlappingWindowNamespacesRoundTrip(
      String kind, boolean stockStore, boolean nativeStore, String shape) throws Exception {
    var plan = new Plan(kind, false, "UTC", shape);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var empty = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      empty.open();
      nativeOp.initializeState(snapshot(empty));
      nativeOp.open();
      try (var reference = plan.stock(stockStore)) {
        reference.initializeState(snapshot(nativeOp));
        reference.open();
        assertTrue(stockOutput(reference).isEmpty());
        assertTrue(nativeOutput(nativeOp).isEmpty());
        plan.prefix(reference);
        plan.prefixNative(nativeOp);
        try (var stockCopy = plan.stock(!stockStore);
            var nativeCopy = plan.nativeHarness(!nativeStore)) {
          stockCopy.initializeState(snapshot(nativeOp));
          stockCopy.open();
          nativeCopy.initializeState(snapshot(reference));
          nativeCopy.open();
          verifyStockMetadata(stockCopy);
          for (long end : new long[] {plan.local + 1000, plan.local + 2000, plan.local + 4000}) {
            long mark = plan.deadline(end);
            plan.watermarks(reference, mark);
            plan.watermarks(stockCopy, mark);
            nativeOp.processWatermark1(new Watermark(mark));
            nativeOp.processWatermark2(new Watermark(mark));
            nativeCopy.processWatermark1(new Watermark(mark));
            nativeCopy.processWatermark2(new Watermark(mark));
            var expected = stockOutput(reference);
            assertEquals(expected, stockOutput(stockCopy));
            assertEquals(expected, nativeOutput(nativeOp));
            assertEquals(expected, nativeOutput(nativeCopy));
          }
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> h) throws Exception {
    h.prepareSnapshotPreBarrier(1);
    return h.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
  }

  static final class Plan {
    final String kind;
    final boolean keyless;
    final String zone;
    final long local;
    final RowDataKeySelector selector;
    final RowType keyType;
    final long size;
    final boolean cumulative;

    Plan(String kind, boolean keyless, String zone) {
      this(kind, keyless, zone, "TUMBLE");
    }

    Plan(String kind, boolean keyless, String zone, String shape) {
      size = shape.equals("CUMULATE") ? 4000 : shape.equals("HOP") ? 2000 : 1000;
      cumulative = shape.equals("CUMULATE");
      this.kind = kind;
      this.keyless = keyless;
      this.zone = zone;
      long clock = TimeWindowUtil.toUtcTimestampMills(BASE, ZoneId.of(zone));
      local =
          cumulative
              ? org.apache.flink.table.runtime.operators.window.TimeWindow.getWindowStartWithOffset(
                  clock, 0, size)
              : clock;
      selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(),
              keyless ? new int[0] : new int[] {0},
              InternalTypeInfo.of(INPUT));
      keyType = ((InternalTypeInfo<RowData>) selector.getProducedType()).toRowType();
    }

    long deadline(long end) {
      return TimeWindowUtil.toEpochMillsForTimer(end - 1, ZoneId.of(zone));
    }

    RowData row(Long key, Long value, long end) {
      long start =
          cumulative
              ? org.apache.flink.table.runtime.operators.window.TimeWindow.getWindowStartWithOffset(
                  end - 1, 0, size)
              : end - size;
      return GenericRowData.of(key, value, start, end);
    }

    List<RowData> left() {
      return List.of(
          row(1L, 10L, local + 1000),
          row(1L, 11L, local + 2000),
          row(1L, 11L, local + 2000),
          row(2L, null, local + 2000),
          row(null, 30L, local + 4000));
    }

    List<RowData> right() {
      return List.of(
          row(1L, 100L, local + 1000),
          row(1L, 200L, local + 2000),
          row(3L, 300L, local + 2000),
          row(null, 400L, local + 4000));
    }

    void prefix(KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
        throws Exception {
      for (var row : left()) h.processElement1(new StreamRecord<>(row));
      for (var row : right()) h.processElement2(new StreamRecord<>(row));
    }

    void prefixNative(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h)
        throws Exception {
      for (var row : left()) push(h, true, row);
      for (var row : right()) push(h, false, row);
    }

    void extra(KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
        throws Exception {
      h.processElement1(new StreamRecord<>(row(1L, 21L, local + 2000)));
      h.processElement2(new StreamRecord<>(row(2L, 220L, local + 2000)));
      h.processElement1(new StreamRecord<>(row(1L, 12L, local + 1000)));
      h.processElement2(new StreamRecord<>(row(1L, 120L, local + 1000)));
    }

    void extraNative(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h)
        throws Exception {
      push(h, true, row(1L, 21L, local + 2000));
      push(h, false, row(2L, 220L, local + 2000));
      push(h, true, row(1L, 12L, local + 1000));
      push(h, false, row(1L, 120L, local + 1000));
    }

    void watermarks(
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h, long mark)
        throws Exception {
      h.processWatermark1(new Watermark(mark));
      h.processWatermark2(new Watermark(mark));
    }

    KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      String code =
          "public class CanonicalWindowJoinCondition extends"
              + " org.apache.flink.api.common.functions.AbstractRichFunction implements"
              + " org.apache.flink.table.runtime.generated.JoinCondition {public"
              + " CanonicalWindowJoinCondition(Object[] refs){}public boolean"
              + " apply(org.apache.flink.table.data.RowData l,org.apache.flink.table.data.RowData"
              + " r){return true;}}";
      var operator =
          WindowJoinOperatorBuilder.builder()
              .leftSerializer(new RowDataSerializer(INPUT))
              .rightSerializer(new RowDataSerializer(INPUT))
              .leftWindowEndIndex(3)
              .rightWindowEndIndex(3)
              .generatedJoinCondition(
                  new GeneratedJoinCondition("CanonicalWindowJoinCondition", code, new Object[0]))
              .filterNullKeys(keyless ? new boolean[0] : new boolean[] {true})
              .joinType(FlinkJoinType.valueOf(kind))
              .withShiftTimezone(ZoneId.of(zone))
              .build();
      var h =
          new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
              operator, selector, selector, selector.getProducedType(), 128, 1, 0);
      if (rocks) h.setStateBackend(tech.streamfusion.compat.CanonicalTestCompat.rocksBackend());
      h.setup(new RowDataSerializer(OUTPUT));
      return h;
    }

    KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
        nativeHarness(boolean rocks) throws Exception {
      long offset =
          ZoneId.of(zone).getRules().isFixedOffset() ? WindowTimeDomain.offsetMillis(zone) : 0;
      var operator =
          new CanonicalJoin(
              keyless ? new int[0] : new int[] {0},
              List.of("INNER", "LEFT", "RIGHT", "FULL").indexOf(kind),
              offset,
              size,
              cumulative);
      operator.withCalendarZone(zone);
      var contract =
          new StockWindowJoinStateContract(
              new RowDataSerializer(keyType),
              keyType,
              INPUT,
              INPUT,
              keyless ? new int[0] : new int[] {0},
              keyless ? new int[0] : new int[] {0},
              2,
              3,
              2,
              3,
              zone,
              cumulative
                  ? StockWindowJoinStateContract.WindowSpec.cumulative(size, 0)
                  : StockWindowJoinStateContract.WindowSpec.fixed(size));
      ClassLoader runtimeLoader =
          new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              if (name.startsWith("org.apache.flink.table.planner."))
                throw new ClassNotFoundException(name);
              return super.loadClass(name, resolve);
            }
          };
      contract =
          org.apache.flink.util.InstantiationUtil.deserializeObject(
              org.apache.flink.util.InstantiationUtil.serializeObject(contract), runtimeLoader);
      operator.setStockStateContract(contract);
      int key = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
      var h =
          new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
              operator, ignored -> key, ignored -> key, Types.INT, 128, 1, 0);
      var config = new Configuration();
      if (rocks)
        config.setString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
            RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          h.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      h.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      h.setup(new ArrowBatchSerializer());
      return h;
    }
  }

  private static final class CanonicalJoin extends NativeWindowJoinOperator {
    private boolean canonicalCapture;

    @Override
    protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures
        snapshotCanonicalState(
            long checkpointId,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory streams)
            throws Exception {
      canonicalCapture = true;
      try {
        return super.snapshotCanonicalState(checkpointId, timestamp, options, streams);
      } finally {
        canonicalCapture = false;
      }
    }

    CanonicalJoin(int[] keys, int kind, long offset, long size, boolean cumulative) {
      super(
          keys,
          keys,
          2,
          3,
          2,
          3,
          kind,
          INPUT,
          INPUT,
          EncodedPredicate.NONE,
          false,
          size,
          1000,
          cumulative,
          keys.length == 0 ? new int[0] : new int[] {-1},
          128,
          offset);
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonicalCapture)
        throw new AssertionError("Canonical window join must not serialize a native snapshot");
      return super.snapshotCanonicalPartitions();
    }
  }

  static void push(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h,
      boolean left,
      RowData row)
      throws Exception {
    var input =
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(List.of(row), INPUT, NativeAllocator.SHARED)));
    if (left) h.processElement1(input);
    else h.processElement2(input);
  }

  static List<String> stockOutput(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h) {
    var result = new ArrayList<String>();
    while (!h.getOutput().isEmpty()) {
      var event = h.getOutput().poll();
      if (event instanceof StreamRecord)
        result.add(format((RowData) ((StreamRecord<?>) event).getValue()));
    }
    result.sort(String::compareTo);
    return result;
  }

  static List<String> nativeOutput(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h) {
    var result = new ArrayList<String>();
    while (!h.getOutput().isEmpty()) {
      var event = h.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) result.add(format(row));
        }
      }
    }
    result.sort(String::compareTo);
    return result;
  }

  static String format(RowData row) {
    var value = new StringBuilder(row.getRowKind().shortString());
    for (int column = 0; column < 8; column++)
      value.append('|').append(row.isNullAt(column) ? "null" : row.getLong(column));
    return value.toString();
  }

  private static void verifyStockMetadata(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock)
      throws Exception {
    var backend =
        (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
            stock.getOperator().<RowData>getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    ClassLoader clean =
        new ClassLoader(CanonicalWindowJoinInteropTest.class.getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("tech.streamfusion.")
                || name.startsWith("org.apache.flink.table.planner."))
              throw new ClassNotFoundException(name);
            return super.loadClass(name, resolve);
          }
        };
    try (AutoCloseable release = resources::release) {
      for (var meta : resources.getMetaInfoSnapshots()) {
        assertTrue(
            Set.of(
                    "left-records",
                    "right-records",
                    "_timer_state/event_window-timers",
                    "_timer_state/processing_window-timers")
                .contains(meta.getName()),
            meta.getName());
        for (var snapshot : meta.getSerializerSnapshotsImmutable().values()) {
          var output = new org.apache.flink.core.memory.DataOutputSerializer(128);
          org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.writeVersionedSnapshot(
              output, snapshot);
          var restored =
              org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.readVersionedSnapshot(
                  new org.apache.flink.core.memory.DataInputDeserializer(output.getCopyOfBuffer()),
                  clean);
          assertTrue(
              restored.restoreSerializer().getClass().getName().startsWith("org.apache.flink."));
        }
      }
    }
  }
}
