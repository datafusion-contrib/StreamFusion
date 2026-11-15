package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupArithmeticAppendInitializationTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticInitializationTest.cases()
        .filter(
            test ->
                System.getProperty("streamfusion.test.appendGroupModes", "").isEmpty()
                    || System.getProperty("streamfusion.test.appendGroupModes")
                        .equals(test.get()[0]));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void actualAppendOperatorsResumeWithoutInventingMissingCounters(
      String mode, boolean rocks, long ttl, boolean first, boolean before) throws Exception {
    var steps =
        List.of(
            row(1L, null, 0),
            row(2L, 11L, 0),
            row(null, 5L, 0),
            row(1L, 7L, 0),
            row(1L, 4L, 0),
            row(2L, -11L, 0),
            row(1L, -7L, 0),
            row(1L, -4L, 0),
            row(null, -5L, 0),
            row(1L, null, 0),
            row(1L, 17L, 0),
            row(2L, null, 0));
    assertLifecycle(mode, rocks, ttl, first, before, true, steps);
  }

  static Stream<Arguments> counters() {
    return Stream.of("star", "count")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .flatMap(
                                    before ->
                                        Stream.of(-1L, Long.MAX_VALUE)
                                            .map(
                                                count ->
                                                    Arguments.of(mode, rocks, before, count)))));
  }

  @ParameterizedTest
  @MethodSource("counters")
  @SuppressWarnings("unchecked")
  void zeroAndWrappedCountsDoNotDeleteAnAppendAccumulator(
      String mode, boolean rocks, boolean before, long initial) throws Exception {
    try (var reference = new GroupAggregateStockTestFixture(mode, rocks, 0, before, true);
        var source = new GroupAggregateStockTestFixture(mode, rocks, 0, before, true)) {
      reference.harness.open();
      source.harness.open();
      for (var fixture : List.of(reference, source)) {
        var backend = fixture.harness.getOperator().<RowData>getKeyedStateBackend();
        backend.setCurrentKey(
            backend.getKeySerializer().copy(fixture.selector.getKey(row(1L, null, 0))));
        ((ValueState<RowData>) fixture.rawState()).update(GenericRowData.of(initial));
        assertEquals(1, fixture.rawStateBytes().size());
      }
      var saved = snapshot(source.harness);
      try (var nativeHarness = nativeHarness(source, rocks)) {
        nativeHarness.initializeState(saved);
        nativeHarness.open();
        int time = 1000;
        for (Long value : new Long[] {7L, null}) {
          assertEquals(
              stockStep(reference, row(1L, value, 0), time),
              nativeStep(nativeHarness, row(1L, value, 0), time, reference.outputType));
          saved = snapshot(nativeHarness);
          assertState(reference, saved, mode, rocks, 0, before, true);
          assertEquals(
              1, reference.rawStateBytes().size(), "zero count is still a live append group");
          time += 100;
        }
      }
      try (var returned = new GroupAggregateStockTestFixture(mode, rocks, 0, before, true)) {
        returned.harness.initializeState(saved);
        returned.harness.open();
        assertEquals(
            stockStep(reference, row(1L, 7L, 0), 1200), stockStep(returned, row(1L, 7L, 0), 1200));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }

  static Stream<Arguments> averageCases() {
    return Stream.of(false, true)
        .flatMap(rocks -> Stream.of(false, true).map(before -> Arguments.of(rocks, before)));
  }

  @ParameterizedTest
  @MethodSource("averageCases")
  @SuppressWarnings("unchecked")
  void nullableSumAndWrappedAverageCountsRemainIndependent(boolean rocks, boolean before)
      throws Exception {
    try (var reference = new GroupAggregateStockTestFixture("hidden", rocks, 0, before, true);
        var source = new GroupAggregateStockTestFixture("hidden", rocks, 0, before, true)) {
      reference.harness.open();
      source.harness.open();
      for (var fixture : List.of(reference, source)) {
        var backend = fixture.harness.getOperator().<RowData>getKeyedStateBackend();
        backend.setCurrentKey(
            backend.getKeySerializer().copy(fixture.selector.getKey(row(1L, null, 0))));
        ((ValueState<RowData>) fixture.rawState())
            .update(GenericRowData.of(Long.MIN_VALUE, Long.MIN_VALUE, -1L));
        assertEquals(1, fixture.rawStateBytes().size());
      }
      var saved = snapshot(source.harness);
      try (var nativeHarness = nativeHarness(source, rocks)) {
        nativeHarness.initializeState(saved);
        nativeHarness.open();
        for (Long value : new Long[] {null, 0L, 1L}) {
          assertEquals(
              stockStep(reference, row(1L, value, 0), 1000),
              nativeStep(nativeHarness, row(1L, value, 0), 1000, reference.outputType));
          saved = snapshot(nativeHarness);
          assertState(reference, saved, "hidden", rocks, 0, before, true);
        }
      }
      try (var returned = new GroupAggregateStockTestFixture("hidden", rocks, 0, before, true)) {
        returned.harness.initializeState(saved);
        returned.harness.open();
        assertEquals(
            stockStep(reference, row(1L, 7L, 0), 1100), stockStep(returned, row(1L, 7L, 0), 1100));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("tech.streamfusion.planner.stream.CanonicalGroupArithmeticRecoveryTest#cases")
  void ordinaryNativeCheckpointsRetainTheCapturedAppendMode(String mode, boolean rocks, long ttl)
      throws Exception {
    CanonicalGroupArithmeticRecoveryTest.assertNativeRecovery(mode, rocks, ttl, true);
  }
}
