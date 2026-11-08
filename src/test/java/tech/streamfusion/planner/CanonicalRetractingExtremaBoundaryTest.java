package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalRetractingExtremaBoundaryTest {
  static Stream<Arguments> cases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .flatMap(
                                    global ->
                                        Stream.of("stock", "native", "checkpoint")
                                            .flatMap(
                                                route ->
                                                    Stream.of("orphan", "cached-null")
                                                        .flatMap(
                                                            trace ->
                                                                Stream.of(0L, 1000L)
                                                                    .filter(
                                                                        ttl ->
                                                                            !trace.equals("orphan")
                                                                                || ttl > 0)
                                                                    .map(
                                                                        ttl ->
                                                                            Arguments.of(
                                                                                mode, rocks, global,
                                                                                route, trace,
                                                                                ttl)))))));
  }

  private static RowData input(boolean global, Long value, int kind) {
    var row = global ? GenericRowData.of(value) : GenericRowData.of(1L, value);
    row.setRowKind(RowKind.fromByteValue((byte) kind));
    return row;
  }

  @ParameterizedTest
  @MethodSource("cases")
  void independentOrphanViewsAndHiddenCachedSelectionsSurviveRealHandoffs(
      String mode, boolean rocks, boolean global, String route, String trace, long ttl)
      throws Exception {
    boolean orphan = trace.equals("orphan");
    Long[] values =
        orphan
            ? new Long[] {null, 1L, 5L, null, 1L, 5L, null, 1L}
            : new Long[] {5L, null, null, 1L, 5L, 1L};
    long[] times =
        orphan
            ? new long[] {900, 1000, 1500, 1900, 2000, 2100, 2900, 4001}
            : new long[] {1000, 1100, 1200, 1300, 1400, 1500};
    int[] kinds = orphan ? new int[] {0, 0, 0, 0, 3, 3, 0, 3} : new int[] {0, 0, 0, 3, 3, 0};
    try (var reference =
        new GroupAggregateStockTestFixture(mode, rocks, ttl, true, false, false, global)) {
      reference.harness.open();
      OperatorSubtaskState checkpoint = null;
      java.util.Map<String, String> expected = null;
      try (var fixture =
              new GroupAggregateStockTestFixture(mode, rocks, ttl, true, false, false, global);
          var target = nativeHarness(fixture, rocks)) {
        if (route.equals("native")) target.open();
        for (int i = 0; i < values.length; i++) {
          var row = input(global, values[i], kinds[i]);
          var expectedUpdates = stockStep(reference, row, times[i]);
          if (route.equals("native"))
            assertEquals(
                expectedUpdates,
                nativeStep(target, row, times[i], fixture.outputType, fixture.inputType));
        }
        expected = reference.rawStateBytes();
        if (orphan) {
          assertEquals(2, expected.size(), "both expired views survive without an accumulator");
          assertTrue(expected.keySet().stream().allMatch(key -> key.startsWith("agg")));
        }
        if (!route.equals("native")) {
          target.setProcessingTime(times[times.length - 1]);
          target.setStateTtlProcessingTime(times[times.length - 1]);
          var saved = snapshot(reference.harness);
          var captured = snapshotContents(saved, mode, rocks, ttl, global);
          assertEquals(
              expected, captured, "immutable stock snapshot contains the actual source views");
          var live = reference.rawStateBytes();
          if (!live.equals(captured))
            System.out.println(
                "STOCK_TTL_SNAPSHOT_REFERENCE mode="
                    + mode
                    + " rocks="
                    + rocks
                    + " global="
                    + global
                    + " snapshot="
                    + captured.size()
                    + " live-before-native-import="
                    + live.size());
          expected = captured;
          target.initializeState(saved);
          target.open();
        }
        if (route.equals("checkpoint")) {
          var owner = (DirectGroup) target.getOperator();
          owner.checkpoint = true;
          try {
            target.prepareSnapshotPreBarrier(10);
            checkpoint = target.snapshot(10, times[times.length - 1]);
          } finally {
            owner.checkpoint = false;
          }
        } else
          verifyAndContinue(reference, fixture, target, mode, rocks, global, orphan, ttl, expected);
      }
      if (checkpoint != null) {
        try (var fixture =
                new GroupAggregateStockTestFixture(mode, rocks, ttl, true, false, false, global);
            var target = nativeHarness(fixture, rocks)) {
          target.setProcessingTime(times[times.length - 1]);
          target.setStateTtlProcessingTime(times[times.length - 1]);
          target.initializeState(checkpoint);
          target.open();
          verifyAndContinue(reference, fixture, target, mode, rocks, global, orphan, ttl, expected);
        }
      }
    }
  }

  private static void verifyAndContinue(
      GroupAggregateStockTestFixture reference,
      GroupAggregateStockTestFixture fixture,
      org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
              Integer, tech.streamfusion.operator.ArrowBatch, tech.streamfusion.operator.ArrowBatch>
          target,
      String mode,
      boolean rocks,
      boolean global,
      boolean orphan,
      long ttl,
      java.util.Map<String, String> expected)
      throws Exception {
    ((DirectGroup) target.getOperator()).assertStorage();
    for (int round = 0; round < 2; round++) {
      assertEquals(
          expected,
          snapshotContents(snapshot(target), mode, rocks, ttl, global),
          "complete canonical state equals the captured source, including expired orphan views");
      assertTrue(target.extractOutputValues().isEmpty(), "no input cannot create output");
    }
    long base = orphan ? 4002 : 1600;
    for (int i = 0; i < 3; i++) {
      var row = input(global, i == 0 ? null : 2L, i == 2 ? 3 : 0);
      long now = base + 100L * i;
      assertEquals(
          stockStep(reference, row, now),
          nativeStep(target, row, now, fixture.outputType, fixture.inputType));
    }
    if (!orphan) assertState(reference, snapshot(target), mode, rocks, ttl, true, false);
  }

  private static java.util.Map<String, String> snapshotContents(
      OperatorSubtaskState saved, String mode, boolean rocks, long ttl, boolean global)
      throws Exception {
    try (var restored =
        new GroupAggregateStockTestFixture(mode, rocks, ttl, true, false, false, global)) {
      restored.harness.setStateTtlProcessingTime(0);
      restored.harness.initializeState(saved);
      restored.harness.open();
      return restored.rawStateBytes();
    }
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .map(
                                                global ->
                                                    Arguments.of(mode, rocks, ttl, global)))));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void ordinaryNativeRecoveryKeepsMapEntriesAndWriteAges(
      String mode, boolean rocks, long ttl, boolean global) throws Exception {
    CanonicalGroupArithmeticRecoveryTest.assertNativeRecovery(mode, rocks, ttl, false, global);
  }

  static Stream<Arguments> emptyCases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of("bigint", "int", "global")
                                            .map(key -> Arguments.of(mode, rocks, ttl, key)))));
  }

  @ParameterizedTest
  @MethodSource("emptyCases")
  void emptyStateCannotInventAnAccumulatorOrAView(String mode, boolean rocks, long ttl, String key)
      throws Exception {
    CanonicalGroupArithmeticEmptyStateTest.assertEmpty(
        mode, rocks, ttl, false, key.equals("int"), key.equals("global"));
  }

  static Stream<Arguments> largeCases() {
    return Stream.of(false, true)
        .flatMap(rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(rocks, ttl)));
  }

  @ParameterizedTest
  @MethodSource("largeCases")
  void oneLargeViewCrossesTransferChunksAndContinuesRetraction(boolean rocks, long ttl)
      throws Exception {
    try (var source = new GroupAggregateStockTestFixture("extrema", rocks, ttl, true, false);
        var target = nativeHarness(source, rocks)) {
      source.harness.open();
      for (long value = 0; value < 1025; value++) stockStep(source, input(false, value, 0), 1000);
      var before = source.rawStateBytes();
      assertEquals(2051, before.size(), "one header and 1025 entries in each of two views");
      target.initializeState(snapshot(source.harness));
      target.open();
      assertState(source, snapshot(target), "extrema", rocks, ttl, true, false);
      assertEquals(before, source.rawStateBytes(), "stock source survives native initialization");
      var row = input(false, 0L, 3);
      assertEquals(
          stockStep(source, row, 1100),
          nativeStep(target, row, 1100, source.outputType, source.inputType));
      assertState(source, snapshot(target), "extrema", rocks, ttl, true, false);
    }
  }
}
