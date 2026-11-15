package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalProcessingTimeWindowOperatorTest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalProcessingTimeSessionOperatorTest {
  private static final long BASE = CanonicalSharedWindowInteropTest.BASE;
  private static final SavepointType CANONICAL =
      SavepointType.savepoint(SavepointFormatType.CANONICAL);

  @BeforeAll
  static void memoryAuthority() {
    CanonicalProcessingTimeWindowOperatorTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
      for (boolean rocks : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (boolean distinct : new boolean[] {false, true})
            cases.add(Arguments.of(zone, rocks, keyless, distinct));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void stockRetiringAnExpiredUnchangedSessionCannotContinueAtItsPendingTimer(
      String zone, boolean rocks, boolean keyless, boolean distinct) throws Exception {
    var plan = plan(zone, keyless, distinct);
    try (var stock = plan.stock(rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      stock.open();
      nativeRun.open();
      stock.setProcessingTime(BASE + 100);
      nativeRun.setProcessingTime(BASE + 100);
      var row = input(-1L, 5L, 100);
      plan.send(stock, row);
      send(plan, nativeRun, row);
      stock.processWatermark(new Watermark(Long.MAX_VALUE - 1));
      nativeRun.processWatermark(new Watermark(Long.MAX_VALUE - 1));
      assertDoesNotThrow(() -> plan.send(stock, row));
      assertThrows(UnsupportedOperationException.class, () -> send(plan, nativeRun, row));
      stock.prepareSnapshotPreBarrier(1);
      var retired = stock.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      try (var invalid = nativeHarness(plan, rocks)) {
        var rejected =
            assertThrows(IllegalStateException.class, () -> invalid.initializeState(retired));
        assertEquals("Stock SESSION timer has no mapping", rejected.getMessage());
      }
      assertThrows(NullPointerException.class, () -> stock.setProcessingTime(BASE + 1099));
    }
  }

  static Stream<Arguments> daylightSavingCases() {
    var cases = new ArrayList<Arguments>();
    String[][] transitions = {
      {"Europe/Berlin", "2024-03-31T00:59:59.500Z"},
      {"Europe/Berlin", "2024-10-27T00:59:59.500Z"},
      {"America/New_York", "2024-03-10T06:59:59.500Z"},
      {"America/New_York", "2024-11-03T05:59:59.500Z"}
    };
    for (var transition : transitions)
      for (boolean rocks : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (boolean distinct : new boolean[] {false, true})
            cases.add(Arguments.of(transition[0], transition[1], rocks, keyless, distinct));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("daylightSavingCases")
  void calendarDiscontinuitiesRetainEveryPendingSessionAcrossBothCanonicalDirections(
      String zone, String instant, boolean rocks, boolean keyless, boolean distinct)
      throws Exception {
    long first = java.time.Instant.parse(instant).toEpochMilli();
    long second = first + 600;
    var plan = plan(zone, keyless, distinct);
    try (var source = plan.stock(rocks);
        var reference = plan.stock(!rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      source.setProcessingTime(first);
      plan.send(source, input(-1L, 5L, first - BASE));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.setProcessingTime(first);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.setProcessingTime(first);
      nativeRun.open();
      reference.setProcessingTime(second);
      nativeRun.setProcessingTime(second);
      var beforeInput = plan.output(reference);
      assertEquals(beforeInput, output(plan, nativeRun));
      var row = input(-1L, 7L, second - BASE);
      plan.send(reference, row);
      send(plan, nativeRun, row);
      // Process any deadline that the calendar placed at/before the current clock.
      reference.setProcessingTime(second);
      nativeRun.setProcessingTime(second);
      var afterInput = plan.output(reference);
      assertEquals(afterInput, output(plan, nativeRun));
      nativeRun.prepareSnapshotPreBarrier(2);
      var afterTransition =
          nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(rocks)) {
        stockAgain.initializeState(afterTransition);
        stockAgain.setProcessingTime(second);
        stockAgain.open();
        int fired = beforeInput.size() + afterInput.size();
        for (long clock : new long[] {second + 999, second + 1100, second + 3_601_000}) {
          reference.setProcessingTime(clock);
          nativeRun.setProcessingTime(clock);
          stockAgain.setProcessingTime(clock);
          var expected = plan.output(reference);
          fired += expected.size();
          assertEquals(expected, output(plan, nativeRun));
          assertEquals(expected, plan.output(stockAgain));
        }
        assertTrue(fired > 0, "The reference must consume retained calendar state");
      }
    }
  }

  private static CanonicalSharedWindowInteropTest.StockPlan plan(
      String zone, boolean keyless, boolean distinct) throws Exception {
    return new CanonicalSharedWindowInteropTest.StockPlan(
        "SESSION", zone, keyless, false, distinct, 0, true);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void mergedSessionSurvivesCanonicalReexportAndAnOrdinaryNativeCheckpoint(
      String zone, boolean rocks, boolean keyless, boolean distinct) throws Exception {
    var plan = plan(zone, keyless, distinct);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      for (long time : new long[] {100, 200, 800}) {
        source.setProcessingTime(BASE + time);
        plan.send(source, input(-1L, time == 200 ? null : 5L, time));
        plan.send(source, input(null, 7L, time));
      }
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.setProcessingTime(BASE + 800);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.setProcessingTime(BASE + 800);
      nativeRun.open();
      var immediate = nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(immediate);
        stockAgain.setProcessingTime(BASE + 800);
        stockAgain.open();
        for (long time : new long[] {900, 1200}) {
          reference.setProcessingTime(BASE + time);
          nativeRun.setProcessingTime(BASE + time);
          stockAgain.setProcessingTime(BASE + time);
          assertEquals(plan.output(reference), output(plan, nativeRun));
          assertEquals(List.of(), plan.output(stockAgain));
          var row = input(-1L, 5L, time);
          plan.send(reference, row);
          plan.send(stockAgain, row);
          send(plan, nativeRun, row);
        }
        nativeRun.prepareSnapshotPreBarrier(3);
        var checkpoint = nativeRun.snapshot(3, 0);
        assertTrue(
            checkpoint.getManagedKeyedState().stream()
                .noneMatch(
                    state ->
                        state instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
        try (var resumed = nativeHarness(plan, rocks)) {
          resumed.initializeState(checkpoint);
          resumed.setProcessingTime(BASE + 1200);
          resumed.open();
          var afterCheckpoint =
              resumed.snapshotWithLocalState(4, 0, CANONICAL).getJobManagerOwnedState();
          try (var stockAfterCheckpoint = plan.stock(!rocks)) {
            stockAfterCheckpoint.initializeState(afterCheckpoint);
            stockAfterCheckpoint.setProcessingTime(BASE + 1200);
            stockAfterCheckpoint.open();
            int fired = 0;
            for (long time : new long[] {1799, 2199, 4999}) {
              reference.setProcessingTime(BASE + time);
              stockAgain.setProcessingTime(BASE + time);
              stockAfterCheckpoint.setProcessingTime(BASE + time);
              resumed.setProcessingTime(BASE + time);
              var expected = plan.output(reference);
              fired += expected.size();
              assertEquals(expected, plan.output(stockAgain));
              assertEquals(expected, plan.output(stockAfterCheckpoint));
              assertEquals(expected, output(plan, resumed));
            }
            assertTrue(fired > 0, "Continuation must emit imported and merged state");
          }
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void restoredAdmissionDropsNewSessionsAndRejectsExpiredMerges(
      String zone, boolean rocks, boolean keyless, boolean distinct) throws Exception {
    var plan = plan(zone, keyless, distinct);
    try (var source = plan.stock(rocks);
        var reference = plan.stock(!rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      source.setProcessingTime(BASE + 100);
      plan.send(source, input(-1L, 5L, 100));
      source.processWatermark(new Watermark(Long.MAX_VALUE - 1));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.setProcessingTime(BASE + 200);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.setProcessingTime(BASE + 200);
      nativeRun.open();
      assertEquals(List.of(), output(plan, nativeRun), "Watermark must not fire clock state");
      if (!keyless) {
        var absent = input(2L, 99L, 200);
        plan.send(reference, absent);
        send(plan, nativeRun, absent);
        var dropped = nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
        try (var stockAgain = plan.stock(rocks)) {
          stockAgain.initializeState(dropped);
          stockAgain.setProcessingTime(BASE + 200);
          stockAgain.open();
          stockAgain.setProcessingTime(BASE + 4999);
          var rows = plan.output(stockAgain);
          assertEquals(1, rows.size());
          assertTrue(rows.get(0).startsWith("-1:1:5:"), rows.toString());
        }
      }
      var merge = input(-1L, 7L, 200);
      var expected =
          assertThrows(UnsupportedOperationException.class, () -> plan.send(reference, merge));
      var actual =
          assertThrows(UnsupportedOperationException.class, () -> send(plan, nativeRun, merge));
      assertEquals(expected.getMessage(), actual.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void advancedAdmissionIsReboundAfterANativeCheckpoint(
      String zone, boolean rocks, boolean keyless, boolean distinct) throws Exception {
    var plan = plan(zone, keyless, distinct);
    try (var reference = plan.stock(!rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      reference.open();
      nativeRun.open();
      reference.setProcessingTime(BASE + 100);
      nativeRun.setProcessingTime(BASE + 100);
      var prefix = input(-1L, 5L, 100);
      plan.send(reference, prefix);
      send(plan, nativeRun, prefix);
      reference.processWatermark(new Watermark(Long.MAX_VALUE - 1));
      nativeRun.processWatermark(new Watermark(Long.MAX_VALUE - 1));
      nativeRun.prepareSnapshotPreBarrier(1);
      var saved = nativeRun.snapshot(1, 0);
      try (var restored = nativeHarness(plan, rocks)) {
        restored.initializeState(saved);
        restored.setProcessingTime(BASE + 200);
        restored.open();
        assertEquals(List.of(), output(plan, restored));
        reference.setProcessingTime(BASE + 200);
        var merge = input(-1L, 7L, 200);
        var expected =
            assertThrows(UnsupportedOperationException.class, () -> plan.send(reference, merge));
        var actual =
            assertThrows(UnsupportedOperationException.class, () -> send(plan, restored, merge));
        assertEquals(expected.getMessage(), actual.getMessage());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void overdueClockStateFiresEvenWhenAdmissionWatermarkIsAhead(
      String zone, boolean rocks, boolean keyless, boolean distinct) throws Exception {
    var plan = plan(zone, keyless, distinct);
    try (var source = plan.stock(rocks);
        var reference = plan.stock(!rocks);
        var nativeRun = nativeHarness(plan, rocks)) {
      source.open();
      source.setProcessingTime(BASE + 100);
      plan.send(source, input(-1L, 5L, 100));
      source.processWatermark(new Watermark(Long.MAX_VALUE - 1));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.setProcessingTime(BASE + 4999);
      reference.open();
      reference.setProcessingTime(BASE + 4999);
      nativeRun.initializeState(saved);
      nativeRun.setProcessingTime(BASE + 4999);
      nativeRun.open();
      var expected = plan.output(reference);
      assertFalse(expected.isEmpty());
      assertEquals(expected, output(plan, nativeRun));
      nativeRun.setProcessingTime(BASE + 9999);
      assertEquals(List.of(), output(plan, nativeRun));
    }
  }
}
