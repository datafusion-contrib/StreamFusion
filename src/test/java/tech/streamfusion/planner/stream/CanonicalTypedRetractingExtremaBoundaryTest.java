package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;
import static tech.streamfusion.planner.stream.CanonicalTypedRetractingExtremaTest.fixture;
import static tech.streamfusion.planner.stream.CanonicalTypedRetractingExtremaTest.row;
import static tech.streamfusion.planner.stream.CanonicalTypedRetractingExtremaTest.values;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalTypedRetractingExtremaBoundaryTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    return CanonicalTypedRetractingExtremaTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of("stock", "native", "checkpoint")
                                .flatMap(
                                    route ->
                                        Stream.of("orphan", "cached", "cached-ttl")
                                            .map(
                                                trace ->
                                                    Arguments.of(type, rocks, route, trace)))));
  }

  @ParameterizedTest(name = "{0},rocks={1},route={2},trace={3}")
  @MethodSource("cases")
  void independentTypedViewsAndHiddenCachesSurviveRestore(
      String type, boolean rocks, String route, String trace) throws Exception {
    boolean orphan = trace.equals("orphan");
    long ttl = trace.equals("cached") ? 0 : 1000;
    var v = values(type);
    Object[] payload =
        orphan
            ? new Object[] {null, v[0], v[2], null, v[0], v[2], null, v[0]}
            : new Object[] {v[2], null, null, v[0], v[2], v[0]};
    long[] times =
        orphan
            ? new long[] {900, 1000, 1500, 1900, 2000, 2100, 2900, 4001}
            : new long[] {1000, 1100, 1200, 1300, 1400, 1500};
    int[] kinds = orphan ? new int[] {0, 0, 0, 0, 3, 3, 0, 3} : new int[] {0, 0, 0, 3, 3, 0};
    try (var reference = fixture(type, rocks, ttl)) {
      reference.harness.open();
      OperatorSubtaskState checkpoint = null;
      Map<String, String> expected;
      try (var source = fixture(type, rocks, ttl);
          var target = nativeHarness(source, rocks)) {
        if (route.equals("native")) target.open();
        for (int i = 0; i < payload.length; i++) {
          var next = row(1L, payload[i], kinds[i]);
          var updates = stockStep(reference, next, times[i]);
          if (route.equals("native"))
            assertEquals(
                updates, nativeStep(target, next, times[i], source.outputType, source.inputType));
        }
        expected = reference.rawStateBytes();
        if (orphan) {
          assertEquals(2, expected.size(), "two raw map entries remain without an accumulator");
          assertTrue(expected.keySet().stream().allMatch(key -> key.startsWith("agg")));
        }
        if (!route.equals("native")) {
          var saved = snapshot(reference.harness);
          expected = contents(saved, type, rocks, ttl);
          target.setProcessingTime(times[times.length - 1]);
          target.setStateTtlProcessingTime(times[times.length - 1]);
          target.initializeState(saved);
          target.open();
        }
        if (route.equals("checkpoint")) {
          var owner = (DirectGroup) target.getOperator();
          owner.checkpoint = true;
          try {
            target.prepareSnapshotPreBarrier(9);
            checkpoint = target.snapshot(9, times[times.length - 1]);
          } finally {
            owner.checkpoint = false;
          }
        } else verify(reference, source, target, type, rocks, ttl, orphan, expected);
      }
      if (checkpoint != null) {
        try (var source = fixture(type, rocks, ttl);
            var target = nativeHarness(source, rocks)) {
          target.setProcessingTime(times[times.length - 1]);
          target.setStateTtlProcessingTime(times[times.length - 1]);
          target.initializeState(checkpoint);
          target.open();
          verify(reference, source, target, type, rocks, ttl, orphan, expected);
        }
      }
    }
  }

  private static Map<String, String> contents(
      OperatorSubtaskState saved, String type, boolean rocks, long ttl) throws Exception {
    try (var returned = fixture(type, rocks, ttl)) {
      returned.harness.setStateTtlProcessingTime(0);
      returned.harness.initializeState(saved);
      returned.harness.open();
      return returned.rawStateBytes();
    }
  }

  private static void verify(
      GroupAggregateStockTestFixture reference,
      GroupAggregateStockTestFixture source,
      org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
              Integer, tech.streamfusion.operator.ArrowBatch, tech.streamfusion.operator.ArrowBatch>
          target,
      String type,
      boolean rocks,
      long ttl,
      boolean orphan,
      Map<String, String> expected)
      throws Exception {
    ((DirectGroup) target.getOperator()).assertStorage();
    for (int i = 0; i < 2; i++) {
      assertEquals(expected, contents(snapshot(target), type, rocks, ttl));
      assertTrue(target.extractOutputValues().isEmpty());
    }
    long base = orphan ? 4002 : 1600;
    var v = values(type);
    for (int i = 0; i < 3; i++) {
      var next = row(1L, i == 0 ? null : v[1], i == 2 ? 3 : 0);
      assertEquals(
          stockStep(reference, next, base + i * 100),
          nativeStep(target, next, base + i * 100, source.outputType, source.inputType));
    }
    if (!orphan)
      CanonicalTypedRetractingExtremaTest.assertState(
          reference, snapshot(target), type, rocks, ttl);
  }
}
