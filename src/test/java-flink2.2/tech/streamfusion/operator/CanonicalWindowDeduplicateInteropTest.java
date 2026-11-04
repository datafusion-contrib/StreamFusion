package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;

class CanonicalWindowDeduplicateInteropTest {
  private static final long BASE = Instant.parse("2024-01-15T12:00:00Z").toEpochMilli();

  @BeforeAll
  static void memoryAuthority() {
    CanonicalWindowRankInteropTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
      for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (boolean keepLast : new boolean[] {false, true})
              cases.add(Arguments.of(shape, zone, rocks, keyless, keepLast));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void firstLastAndEqualTimeSelectionsContinueAcrossCanonicalAndNativeCheckpoints(
      String shape, String zone, boolean rocks, boolean keyless, boolean keepLast)
      throws Exception {
    var plan = plan(shape, zone, keyless, keepLast);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = plan.nativeHarness(rocks)) {
      source.open();
      long local = TimeWindowUtil.toUtcTimestampMills(BASE, ZoneId.of(zone));
      long[] ends =
          shape.equals("CUMULATE")
              ? new long[] {local + 1000, local + 2000, local + 3000, local + 6000}
              : new long[] {local + 3000, local + 6000};
      long tag = 0;
      for (long end : ends)
        for (Long key : new Long[] {-1L, null, 2L})
          for (long beforeEnd : new long[] {700, 400, 400}) {
            long rt = TimeWindowUtil.toEpochMills(end - beforeEnd, ZoneId.of(zone));
            plan.send(source, row(plan, key, tag++, end, rt));
          }
      source.processWatermark(new Watermark(BASE + 500));
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(1, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      assertEquals(
          rocks, ((NativeColumnarWindowRankOperator) nativeRun.getOperator()).directRocksDBState());
      nativeRun.prepareSnapshotPreBarrier(2);
      var exported =
          nativeRun
              .snapshotWithLocalState(2, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(exported);
        stockAgain.open();
        for (long end : ends)
          for (long beforeEnd : new long[] {800, 100, 100}) {
            var row =
                row(
                    plan,
                    -1L,
                    tag++,
                    end,
                    TimeWindowUtil.toEpochMills(end - beforeEnd, ZoneId.of(zone)));
            plan.send(reference, row);
            plan.send(stockAgain, row);
            plan.sendNative(nativeRun, row);
          }
        nativeRun.prepareSnapshotPreBarrier(3);
        var checkpoint = nativeRun.snapshot(3, 0);
        assertTrue(
            checkpoint.getManagedKeyedState().stream()
                .noneMatch(
                    state ->
                        state instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
        assertFalse(
            checkpoint.getManagedKeyedState().isEmpty() && checkpoint.getRawKeyedState().isEmpty());
        try (var resumed = plan.nativeHarness(rocks)) {
          resumed.initializeState(checkpoint);
          resumed.open();
          resumed.prepareSnapshotPreBarrier(4);
          var roundTrip =
              resumed
                  .snapshotWithLocalState(4, 0, CanonicalWindowRankInteropTest.CANONICAL)
                  .getJobManagerOwnedState();
          try (var stockAfterCheckpoint = plan.stock(!rocks)) {
            stockAfterCheckpoint.initializeState(roundTrip);
            stockAfterCheckpoint.open();
            int emitted = 0;
            for (long wm :
                new long[] {BASE + 999, BASE + 1999, BASE + 2999, BASE + 5999, Long.MAX_VALUE}) {
              reference.processWatermark(new Watermark(wm));
              stockAgain.processWatermark(new Watermark(wm));
              stockAfterCheckpoint.processWatermark(new Watermark(wm));
              resumed.processWatermark(new Watermark(wm));
              var expected = plan.output(reference);
              emitted += expected.size();
              assertEquals(expected, plan.output(stockAgain));
              assertEquals(expected, plan.output(stockAfterCheckpoint));
              assertEquals(expected, plan.nativeOutput(resumed));
              if (wm == BASE + 2999) {
                var late = row(plan, 77L, tag++, ends[0], BASE + 100);
                plan.send(reference, late);
                plan.send(stockAgain, late);
                plan.send(stockAfterCheckpoint, late);
                plan.sendNative(resumed, late);
              }
            }
            assertTrue(emitted > 0);
          }
        }
      }
    }
  }

  static Stream<Arguments> emptyCases() {
    return Stream.of("UTC", "+05:45", "Europe/Berlin", "America/New_York")
        .flatMap(zone -> Stream.of(Arguments.of(zone, false), Arguments.of(zone, true)));
  }

  static Stream<Arguments> corruptions() {
    return Stream.of(
            "missingTimer",
            "wrongTimer",
            "processingTimer",
            "missingState",
            "orphanTimer",
            "wrongEnd",
            "wrongStart",
            "wrongPartition",
            "retraction",
            "nullOrder",
            "unknownState")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        stockRocks ->
                            Stream.of(
                                Arguments.of(mode, stockRocks, false),
                                Arguments.of(mode, stockRocks, true))));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void malformedValueStateAndTimersFailBeforeImportingRows(
      String mode, boolean stockRocks, boolean rocks) throws Exception {
    var plan = plan("TUMBLE", "UTC", false, false);
    try (var source = plan.stock(!stockRocks);
        var view = plan.view(stockRocks);
        var target = plan.nativeHarness(rocks)) {
      source.open();
      plan.send(source, row(plan, 1L, 1, BASE + 3000, BASE + 100));
      var original = row(plan, 2L, 2, BASE + 6000, BASE + 3100);
      plan.send(source, original);
      source.processWatermark(new Watermark(BASE + 500));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source
              .snapshotWithLocalState(1, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState());
      view.open();
      var backend =
          (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var key = plan.selector.getKey(original);
      backend.setCurrentKey(key);
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      var state =
          backend.getPartitionedState(
              BASE + 6000,
              longs,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "window_deduplicate",
                  new org.apache.flink.table.runtime.typeutils.RowDataSerializer(plan.input)));
      var timers =
          backend.create(
              "_timer_state/event_window-timers",
              new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                  plan.selector.getProducedType().toSerializer(), longs));
      if (mode.equals("missingState")) state.clear();
      else if (mode.equals("unknownState"))
        backend
            .getPartitionedState(
                BASE + 6000,
                longs,
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "unowned-window-dedup-state", longs))
            .update(1L);
      else if (mode.equals("missingTimer") || mode.equals("wrongTimer")) {
        var timer = timers.poll();
        assertNotNull(timer);
        if (mode.equals("wrongTimer"))
          timers.add(
              new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                  timer.getTimestamp() + 1, timer.getKey(), timer.getNamespace()));
      } else if (mode.equals("processingTimer"))
        backend
            .create(
                "_timer_state/processing_window-timers",
                new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                    plan.selector.getProducedType().toSerializer(), longs))
            .add(
                new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                    BASE + 5999, key, BASE + 6000));
      else if (mode.equals("orphanTimer"))
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                BASE + 8999, key, BASE + 9000));
      else {
        var changed = new GenericRowData(plan.input.getFieldCount());
        for (int i = 0; i < changed.getArity(); i++)
          changed.setField(
              i, RowData.createFieldGetter(plan.input.getTypeAt(i), i).getFieldOrNull(original));
        if (mode.equals("wrongEnd"))
          changed.setField(plan.end, TimestampData.fromEpochMillis(BASE + 6001));
        else if (mode.equals("wrongStart"))
          changed.setField(plan.start, TimestampData.fromEpochMillis(BASE + 3001));
        else if (mode.equals("wrongPartition")) changed.setField(plan.partitions[0], 999L);
        else if (mode.equals("retraction"))
          changed.setRowKind(org.apache.flink.types.RowKind.DELETE);
        else if (mode.equals("nullOrder")) changed.setField(plan.sortColumns[0], null);
        else fail(mode);
        state.update(changed);
      }
      var damaged =
          view.snapshotWithLocalState(2, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      Throwable cause = assertThrows(Exception.class, () -> target.initializeState(damaged));
      while (cause.getCause() != null) cause = cause.getCause();
      assertInstanceOf(IllegalStateException.class, cause);
      var operator = (NativeColumnarWindowRankOperator) target.getOperator();
      if (mode.equals("processingTimer") || mode.equals("unknownState")) {
        assertTrue(
            cause
                .getMessage()
                .contains(
                    mode.equals("processingTimer")
                        ? "native adapter does not consume these pending timers"
                        : "descriptor is not owned by the selected native adapter"),
            cause.toString());
        assertEquals(0, operator.handle);
      } else {
        assertTrue(cause.getMessage().contains("window-rank"), cause.toString());
        assertNotEquals(0, operator.handle);
        assertEquals(rocks, operator.directRocksDBState());
        var imported = new java.util.concurrent.atomic.AtomicInteger();
        Native.exportWindowRankerCanonical(
            operator.handle,
            (arrayAddress, schemaAddress) -> {
              try (var array = org.apache.arrow.c.ArrowArray.wrap(arrayAddress);
                  var schema = org.apache.arrow.c.ArrowSchema.wrap(schemaAddress);
                  var root =
                      org.apache.arrow.c.Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
                imported.addAndGet(root.getRowCount());
              }
            });
        assertEquals(0, imported.get());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("emptyCases")
  void emptyValueStateStillRestoresWatermarkBeforeLateInput(String zone, boolean rocks)
      throws Exception {
    var plan = plan("TUMBLE", zone, false, true);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = plan.nativeHarness(rocks)) {
      source.open();
      source.processWatermark(new Watermark(BASE + 2999));
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(1, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      nativeRun.prepareSnapshotPreBarrier(2);
      var exported =
          nativeRun
              .snapshotWithLocalState(2, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(exported);
        stockAgain.open();
        nativeRun.processWatermark(new Watermark(BASE - 1));
        long local = TimeWindowUtil.toUtcTimestampMills(BASE, ZoneId.of(zone));
        for (long end : new long[] {local + 3000, local + 6000}) {
          var row = row(plan, 1L, end, end, TimeWindowUtil.toEpochMills(end - 1, ZoneId.of(zone)));
          plan.send(reference, row);
          plan.send(stockAgain, row);
          plan.sendNative(nativeRun, row);
        }
        reference.processWatermark(Watermark.MAX_WATERMARK);
        stockAgain.processWatermark(Watermark.MAX_WATERMARK);
        nativeRun.processWatermark(Watermark.MAX_WATERMARK);
        var expected = plan.output(reference);
        assertEquals(1, expected.size());
        assertEquals(expected, plan.output(stockAgain));
        assertEquals(expected, plan.nativeOutput(nativeRun));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void manyActualKeysCrossTransferChunksWithoutCollapsingValueState(boolean rocks)
      throws Exception {
    var plan = plan("TUMBLE", "UTC", false, true);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = plan.nativeHarness(rocks)) {
      source.open();
      for (long key = 0; key < 2500; key++)
        plan.send(source, row(plan, key, key, BASE + 3000, BASE + 100));
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(1, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      nativeRun.prepareSnapshotPreBarrier(2);
      var exported =
          nativeRun
              .snapshotWithLocalState(2, 0, CanonicalWindowRankInteropTest.CANONICAL)
              .getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(exported);
        stockAgain.open();
        for (long key = 0; key < 2500; key++) {
          var row = row(plan, key, key + 2500, BASE + 3000, BASE + 100);
          plan.send(reference, row);
          plan.send(stockAgain, row);
          plan.sendNative(nativeRun, row);
        }
        reference.processWatermark(Watermark.MAX_WATERMARK);
        stockAgain.processWatermark(Watermark.MAX_WATERMARK);
        nativeRun.processWatermark(Watermark.MAX_WATERMARK);
        var expected = plan.output(reference);
        assertEquals(2500, expected.size());
        assertEquals(expected, plan.output(stockAgain));
        assertEquals(expected, plan.nativeOutput(nativeRun));
      }
    }
  }

  private static CanonicalWindowRankInteropTest.StockPlan plan(
      String shape, String zone, boolean keyless, boolean keepLast) throws Exception {
    var plan =
        new CanonicalWindowRankInteropTest.StockPlan(shape, zone, keyless, keepLast, false, true);
    assertTrue(plan.json.contains("stream-exec-window-deduplicate"), plan.json);
    assertEquals(
        java.util.Map.of(
            "window_deduplicate", org.apache.flink.api.common.state.StateDescriptor.Type.VALUE),
        plan.contract().stateDescriptors());
    return plan;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void incompatibleRankLimitCannotOverwriteTheSelectedStockValue(boolean rocks) throws Exception {
    var plan = plan("TUMBLE", "UTC", false, false);
    try (var nativeRun = plan.nativeHarness(rocks, 3)) {
      nativeRun.open();
      plan.sendNative(nativeRun, row(plan, 1L, 1, BASE + 3000, BASE + 100));
      plan.sendNative(nativeRun, row(plan, 1L, 2, BASE + 3000, BASE + 200));
      nativeRun.prepareSnapshotPreBarrier(1);
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      Throwable cause =
          assertThrows(
              Exception.class,
              () ->
                  nativeRun.snapshotWithLocalState(1, 0, CanonicalWindowRankInteropTest.CANONICAL));
      while (cause.getCause() != null) cause = cause.getCause();
      assertTrue(
          cause.getMessage().contains("Multiple retained rows for stock window deduplication"),
          cause.toString());
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      var operator = (NativeColumnarWindowRankOperator) nativeRun.getOperator();
      var retained = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportWindowRankerCanonical(
          operator.handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = org.apache.arrow.c.ArrowArray.wrap(arrayAddress);
                var schema = org.apache.arrow.c.ArrowSchema.wrap(schemaAddress);
                var root =
                    org.apache.arrow.c.Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              retained.addAndGet(root.getRowCount());
            }
          });
      assertEquals(2, retained.get(), "Failed canonical capture must not prune live native state");
    }
  }

  private static RowData row(
      CanonicalWindowRankInteropTest.StockPlan plan, Long key, long tag, long end, long rowtime) {
    var original = plan.row(key, null, tag, end);
    var row = new GenericRowData(plan.input.getFieldCount());
    for (int i = 0; i < row.getArity(); i++)
      row.setField(
          i, RowData.createFieldGetter(plan.input.getTypeAt(i), i).getFieldOrNull(original));
    row.setField(plan.sortColumns[0], TimestampData.fromEpochMillis(rowtime));
    return new org.apache.flink.table.runtime.typeutils.RowDataSerializer(plan.input)
        .toBinaryRow(row)
        .copy();
  }
}
