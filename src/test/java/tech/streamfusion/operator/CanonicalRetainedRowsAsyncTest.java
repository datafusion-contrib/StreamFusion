package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class CanonicalRetainedRowsAsyncTest {
  @BeforeAll
  static void memory() {
    CanonicalTemporalSortInteropTest.memory();
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "row", "map"})
  void nestedSortViewsFinishAfterLiveWritesAndNativeOwnerClose(String type) throws Exception {
    var plan = new CanonicalTemporalSortInteropTest.Plan(type, 128);
    var captured = new OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    try (var stock = plan.stock(false);
        var source = plan.nativeHarness(true)) {
      try {
        stock.open();
        source.open();
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertTrue(owner.directRocksDBState(), "nested payloads must use native RocksDB");
        for (int stage = 0; stage < 3; stage++) {
          for (int index = 0; index < 3; index++) {
            var row = plan.row(stage * 3 + index, 100 + stage * 100L);
            plan.pushStock(stock, row);
            plan.push(source, row);
          }
          if (stage < 2) {
            expected[stage] = CanonicalTemporalSortInteropTest.snapshot(stock);
            captured[stage] = CanonicalDistinctWindowAsyncTest.capture(source, 10 + stage);
          }
        }
        source.prepareSnapshotPreBarrier(12);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var reference = plan.stock(false);
              var returned = plan.stock(false)) {
            reference.initializeState(expected[stage]);
            returned.initializeState(saved);
            reference.open();
            returned.open();
            reference.processWatermark(new Watermark(1000));
            returned.processWatermark(new Watermark(1000));
            var expectedRows = plan.stockOutput(reference);
            assertFalse(expectedRows.isEmpty());
            assertEquals(expectedRows, plan.stockOutput(returned));
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
      }
    }
  }

  static Stream<Arguments> rankedTypes() {
    return Stream.of("ARRAY<BIGINT>", "ROW<x BIGINT, y STRING>", "MAP<STRING,BIGINT>")
        .flatMap(type -> Stream.of(false, true).map(dedup -> Arguments.of(type, dedup)));
  }

  @ParameterizedTest
  @MethodSource("rankedTypes")
  void nestedWindowRankViewsFinishAfterLiveWritesAndNativeOwnerClose(String type, boolean dedup)
      throws Exception {
    var plan =
        new CanonicalWindowRankInteropTest.StockPlan(
            "TUMBLE", "UTC", false, false, false, dedup, type);
    var captured = new OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    try (var stock = plan.stock(false);
        var source = plan.nativeHarness(true)) {
      try {
        stock.open();
        source.open();
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertTrue(owner.directRocksDBState());
        for (int stage = 0; stage < 3; stage++) {
          for (int index = 0; index < 3; index++) {
            Object payload;
            if (index == 0) payload = null;
            else if (type.startsWith("ARRAY"))
              payload = new GenericArrayData(new Long[] {(long) stage, null, (long) index});
            else if (type.startsWith("ROW"))
              payload = GenericRowData.of((long) stage, StringData.fromString("value-" + index));
            else {
              var map = new LinkedHashMap<StringData, Long>();
              map.put(StringData.fromString("value-" + index), (long) stage);
              map.put(StringData.fromString("null"), null);
              payload = new GenericMapData(map);
            }
            var row =
                plan.rowWithPayload(
                    index == 0 ? null : 7L,
                    (long) (20 - stage * 3 - index),
                    payload,
                    CanonicalWindowRankInteropTest.BASE + 3000);
            plan.send(stock, row);
            plan.sendNative(source, row);
          }
          if (stage < 2) {
            stock.prepareSnapshotPreBarrier(10 + stage);
            expected[stage] =
                stock
                    .snapshotWithLocalState(10 + stage, 0, CanonicalWindowRankInteropTest.CANONICAL)
                    .getJobManagerOwnedState();
            captured[stage] = CanonicalDistinctWindowAsyncTest.capture(source, 10 + stage);
          }
        }
        source.prepareSnapshotPreBarrier(12);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var reference = plan.stock(false);
              var returned = plan.stock(false)) {
            reference.initializeState(expected[stage]);
            returned.initializeState(saved);
            reference.open();
            returned.open();
            reference.processWatermark(Watermark.MAX_WATERMARK);
            returned.processWatermark(Watermark.MAX_WATERMARK);
            var expectedRows = serialized(reference, plan.output);
            assertFalse(expectedRows.isEmpty());
            assertEquals(expectedRows, serialized(returned, plan.output));
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
      }
    }
  }

  static Stream<Arguments> overTypes() {
    return Stream.of("ARRAY<BIGINT>", "ROW<x BIGINT, y STRING>", "MAP<STRING,BIGINT>")
        .flatMap(
            type ->
                Stream.of(
                        "range",
                        "event-rows",
                        "processing-rows",
                        "unbounded-range",
                        "unbounded-rows")
                    .map(mode -> Arguments.of(type, mode)));
  }

  @ParameterizedTest
  @MethodSource("overTypes")
  void nestedOverViewsFinishAfterLiveWritesAndNativeOwnerClose(String type, String mode)
      throws Exception {
    boolean unbounded = mode.startsWith("unbounded"), processing = mode.equals("processing-rows");
    var fold =
        unbounded
            ? new CanonicalEventUnboundedOverInteropTest.Plan(
                "BIGINT", mode.endsWith("rows"), false, 0, false, false, false, type)
            : null;
    var bounded =
        unbounded
            ? null
            : new CanonicalBoundedRangeInteropTest.Plan(
                "BIGINT",
                false,
                processing,
                mode.equals("event-rows"),
                processing || mode.equals("event-rows") ? 2 : 32,
                0,
                false,
                type);
    var input = unbounded ? fold.input : bounded.input;
    var output = unbounded ? fold.output : bounded.output;
    var captured = new OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    try (var stock = unbounded ? fold.stock(false) : bounded.stock(false);
        var source = unbounded ? fold.nativeHarness(true) : bounded.nativeHarness(true)) {
      try {
        stock.open();
        source.open();
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertTrue(owner.directRocksDBState(), "nested OVER payloads must use native RocksDB");
        for (int stage = 0; stage < 3; stage++) {
          if (processing) {
            stock.setProcessingTime(1000 + stage * 100);
            source.setProcessingTime(1000 + stage * 100);
          }
          for (int index = 0; index < 3; index++) {
            var row =
                (GenericRowData)
                    (unbounded
                        ? fold.row(
                            index == 0 ? null : 7L,
                            (long) (stage * 10 + index),
                            1000 + stage * 100 + index)
                        : bounded.row(
                            index == 0 ? null : 7L,
                            stage * 10 + index,
                            1000 + stage * 100 + index));
            row.setField(input.getFieldNames().indexOf("payload"), payload(type, stage, index));
            stock.processElement(
                new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
            source.processElement(
                new StreamRecord<>(
                    new ArrowBatch(
                        RowDataArrowConverter.write(
                            List.of(row), input, NativeAllocator.SHARED, true))));
          }
          if (!processing) {
            stock.processWatermark(new Watermark(1001 + stage * 100));
            source.processWatermark(new Watermark(1001 + stage * 100));
          }
          discardNativeOutput(source);
          if (stage < 2) {
            expected[stage] =
                stock
                    .snapshotWithLocalState(10 + stage, 0, CanonicalWindowRankInteropTest.CANONICAL)
                    .getJobManagerOwnedState();
            captured[stage] = CanonicalDistinctWindowAsyncTest.capture(source, 10 + stage);
          }
        }
        source.prepareSnapshotPreBarrier(12);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var reference = unbounded ? fold.stock(false) : bounded.stock(false);
              var returned = unbounded ? fold.stock(false) : bounded.stock(false)) {
            reference.initializeState(expected[stage]);
            returned.initializeState(saved);
            reference.open();
            returned.open();
            if (processing) {
              reference.setProcessingTime(2000);
              returned.setProcessingTime(2000);
            }
            var suffix =
                (GenericRowData) (unbounded ? fold.row(7L, 99L, 2000) : bounded.row(7L, 99, 2000));
            suffix.setField(input.getFieldNames().indexOf("payload"), payload(type, 9, 2));
            for (var h : List.of(reference, returned)) {
              h.processElement(
                  new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(suffix).copy()));
              if (!processing) h.processWatermark(Watermark.MAX_WATERMARK);
            }
            var rows = serialized(reference, output);
            assertFalse(rows.isEmpty());
            assertEquals(rows, serialized(returned, output));
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
      }
    }
  }

  static Stream<Arguments> joinTypes() {
    return Stream.of("ARRAY<BIGINT>", "ROW<x BIGINT, y STRING>", "MAP<STRING,BIGINT>")
        .flatMap(
            type ->
                Stream.of("interval-full", "temporal-inner", "temporal-left")
                    .map(mode -> Arguments.of(type, mode)));
  }

  @ParameterizedTest
  @MethodSource("joinTypes")
  void nestedJoinViewsFinishAfterLiveWritesAndNativeOwnerClose(String type, String mode)
      throws Exception {
    boolean interval = mode.startsWith("interval"), outer = !mode.endsWith("inner");
    var valueType =
        org.apache.flink.table.types.logical.utils.LogicalTypeParser.parse(
            type, getClass().getClassLoader());
    var plan = new CanonicalIntervalJoinInteropTest.Plan(3, false, "long", false, valueType);
    var input = plan.input;
    var captured = new OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    try (var stock =
            interval
                ? plan.stock(false)
                : CanonicalTemporalMetadataFixtureTest.harness(false, 0, outer, input);
        var source =
            interval
                ? plan.nativeHarness(true)
                : CanonicalTemporalJoinInteropTest.nativeHarness(true, 0, outer, input)) {
      try {
        stock.open();
        source.open();
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertTrue(owner.directRocksDBState(), "nested join payloads must use native RocksDB");
        for (int stage = 0; stage < 3; stage++) {
          for (int index = 0; index < 3; index++) {
            Long key = index == 0 ? null : 7L;
            var right =
                GenericRowData.of(
                    key,
                    (long) (stage * 10 + index),
                    interval ? 1000L + stage * 100 : 100L + stage * 100,
                    payload(type, stage, index));
            var left =
                GenericRowData.of(
                    key,
                    (long) (stage * 20 + index),
                    1000L + stage * 100,
                    payload(type, stage, 2 - index));
            stock.processElement2(
                new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(right).copy()));
            source.processElement2(
                new StreamRecord<>(
                    new ArrowBatch(
                        RowDataArrowConverter.write(
                            List.of(right), input, NativeAllocator.SHARED, true))));
            stock.processElement1(
                new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(left).copy()));
            source.processElement1(
                new StreamRecord<>(
                    new ArrowBatch(
                        RowDataArrowConverter.write(
                            List.of(left), input, NativeAllocator.SHARED, true))));
          }
          discardNativeOutput(source);
          if (stage < 2) {
            expected[stage] =
                stock
                    .snapshotWithLocalState(10 + stage, 0, CanonicalWindowRankInteropTest.CANONICAL)
                    .getJobManagerOwnedState();
            captured[stage] = CanonicalDistinctWindowAsyncTest.capture(source, 10 + stage);
          }
        }
        source.prepareSnapshotPreBarrier(12);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var reference =
                  interval
                      ? plan.stock(false)
                      : CanonicalTemporalMetadataFixtureTest.harness(false, 0, outer, input);
              var returned =
                  interval
                      ? plan.stock(false)
                      : CanonicalTemporalMetadataFixtureTest.harness(false, 0, outer, input)) {
            reference.initializeState(expected[stage]);
            returned.initializeState(saved);
            reference.open();
            returned.open();
            var suffix =
                GenericRowData.of(
                    7L, 99L, interval ? 1000L + stage * 100 : 900L, payload(type, 9, 2));
            for (var h : List.of(reference, returned)) {
              h.processElement2(
                  new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(suffix).copy()));
              h.processBothWatermarks(new Watermark(5000));
            }
            var rows = serialized(reference, plan.output);
            assertFalse(rows.isEmpty());
            assertEquals(rows, serialized(returned, plan.output));
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
      }
    }
  }

  private static void discardNativeOutput(AbstractStreamOperatorTestHarness<?> source) {
    while (!source.getOutput().isEmpty()) {
      var event = source.getOutput().poll();
      if (event instanceof StreamRecord<?>
          && ((StreamRecord<?>) event).getValue() instanceof ArrowBatch)
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
    }
  }

  private static Object payload(String type, int stage, int index) {
    if (index == 0) return null;
    if (type.startsWith("ARRAY"))
      return new GenericArrayData(new Long[] {(long) stage, null, (long) index});
    if (type.startsWith("ROW"))
      return GenericRowData.of((long) stage, StringData.fromString("value-" + index));
    var map = new LinkedHashMap<StringData, Long>();
    map.put(StringData.fromString("value-" + index), (long) stage);
    map.put(StringData.fromString("null"), null);
    return new GenericMapData(map);
  }

  private static List<String> serialized(
      AbstractStreamOperatorTestHarness<?> harness, RowType type) {
    var serializer = new RowDataSerializer(type);
    var rows = new ArrayList<String>();
    for (var event : harness.getOutput()) {
      if (event instanceof StreamRecord<?>) {
        var value = ((StreamRecord<?>) event).getValue();
        var row = serializer.toBinaryRow((RowData) value);
        rows.add(
            Base64.getEncoder()
                .encodeToString(
                    BinarySegmentUtils.copyToBytes(
                        row.getSegments(), row.getOffset(), row.getSizeInBytes())));
      }
    }
    Collections.sort(rows);
    return rows;
  }
}
