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
