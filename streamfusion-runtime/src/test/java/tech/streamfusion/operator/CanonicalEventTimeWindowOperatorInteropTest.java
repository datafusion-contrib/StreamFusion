package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalProcessingTimeWindowOperatorTest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalEventTimeWindowOperatorInteropTest {
  private static final long BASE = CanonicalSharedWindowInteropTest.BASE;
  private static final org.apache.flink.runtime.checkpoint.SavepointType CANONICAL =
      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
          org.apache.flink.core.execution.SavepointFormatType.CANONICAL);

  @BeforeAll
  static void memoryAuthority() {
    CanonicalProcessingTimeWindowOperatorTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE", "SESSION"))
      for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (boolean distinct : new boolean[] {false, true})
              for (boolean integerKey :
                  keyless ? new boolean[] {false} : new boolean[] {false, true})
                cases.add(Arguments.of(shape, zone, rocks, keyless, distinct, integerKey));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void actualEventTimeOperatorsPreservePendingStateProgressAndEmptyReturns(
      String shape,
      String zone,
      boolean rocks,
      boolean keyless,
      boolean distinct,
      boolean integerKey)
      throws Exception {
    var plan =
        new CanonicalSharedWindowInteropTest.StockPlan(
            shape, zone, keyless, false, distinct, 0, false, integerKey);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      for (var row :
          List.of(
              input(-1L, 5L, 100),
              input(-1L, 5L, 200),
              input(null, 7L, 800),
              input(2L, null, 900),
              input((long) Integer.MIN_VALUE, 3L, 200),
              input((long) Integer.MAX_VALUE, 9L, 800))) plan.send(source, row);
      source.processWatermark(new Watermark(BASE + 500));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      nativeRun.prepareSnapshotPreBarrier(2);
      var returned = nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(returned);
        stockAgain.open();
        nativeRun.processWatermark(new Watermark(BASE + 100));
        for (var row :
            List.of(
                input(-1L, 2L, 700),
                input(null, 3L, 1500),
                input(2L, 4L, 2200),
                input(-1L, 8L, 3500))) {
          plan.send(reference, row);
          plan.send(stockAgain, row);
          send(plan, nativeRun, row);
        }
        reference.processWatermark(new Watermark(BASE + 1200));
        stockAgain.processWatermark(new Watermark(BASE + 1200));
        nativeRun.processWatermark(new Watermark(BASE + 1200));
        var expected = plan.output(reference);
        assertEquals(expected, plan.output(stockAgain));
        assertEquals(expected, output(plan, nativeRun));
        nativeRun.prepareSnapshotPreBarrier(3);
        var checkpoint = nativeRun.snapshot(3, 0);
        reference.prepareSnapshotPreBarrier(3);
        stockAgain.prepareSnapshotPreBarrier(3);
        var referenceCheckpoint =
            reference.snapshotWithLocalState(3, 0, CANONICAL).getJobManagerOwnedState();
        var stockCheckpoint =
            stockAgain.snapshotWithLocalState(3, 0, CANONICAL).getJobManagerOwnedState();
        try (var resumed = nativeHarness(plan, rocks);
            var restoredReference = plan.stock(rocks);
            var restoredStock = plan.stock(!rocks)) {
          resumed.initializeState(checkpoint);
          resumed.open();
          restoredReference.initializeState(referenceCheckpoint);
          restoredReference.open();
          restoredStock.initializeState(stockCheckpoint);
          restoredStock.open();
          resumed.prepareSnapshotPreBarrier(4);
          var afterCheckpoint =
              resumed.snapshotWithLocalState(4, 0, CANONICAL).getJobManagerOwnedState();
          try (var stockAfterCheckpoint = plan.stock(!rocks)) {
            stockAfterCheckpoint.initializeState(afterCheckpoint);
            stockAfterCheckpoint.open();
            var late = input(999L, 100L, 100);
            plan.send(restoredReference, late);
            plan.send(restoredStock, late);
            plan.send(stockAfterCheckpoint, late);
            send(plan, resumed, late);
            restoredReference.processWatermark(new Watermark(BASE + 5999));
            restoredStock.processWatermark(new Watermark(BASE + 5999));
            stockAfterCheckpoint.processWatermark(new Watermark(BASE + 5999));
            resumed.processWatermark(new Watermark(BASE + 5999));
            expected = plan.output(restoredReference);
            assertFalse(expected.isEmpty());
            assertEquals(expected, plan.output(restoredStock));
            assertEquals(expected, plan.output(stockAfterCheckpoint));
            assertEquals(expected, output(plan, resumed));
            resumed.prepareSnapshotPreBarrier(5);
            var empty = resumed.snapshotWithLocalState(5, 0, CANONICAL).getJobManagerOwnedState();
            try (var emptyNative = nativeHarness(plan, rocks);
                var emptyStock = plan.stock(!rocks)) {
              emptyNative.initializeState(empty);
              emptyNative.open();
              emptyStock.initializeState(empty);
              emptyStock.open();
              emptyNative.processWatermark(new Watermark(BASE + 1));
              var closed = input(777L, 77L, 1000);
              plan.send(restoredReference, closed);
              plan.send(emptyStock, closed);
              send(plan, emptyNative, closed);
              restoredReference.processWatermark(new Watermark(BASE + 9999));
              emptyStock.processWatermark(new Watermark(BASE + 9999));
              emptyNative.processWatermark(new Watermark(BASE + 9999));
              assertEquals(List.of(), plan.output(restoredReference));
              assertEquals(List.of(), plan.output(emptyStock));
              assertEquals(List.of(), output(plan, emptyNative));
            }
          }
        }
      }
    }
  }
}
