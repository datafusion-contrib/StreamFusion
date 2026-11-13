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

class CanonicalGroupArithmeticAverageOverflowTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(rocks -> Stream.of(false, true).map(before -> Arguments.of(rocks, before)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @SuppressWarnings("unchecked")
  void restoredAverageDividesLikeReleasedJavaAtTheSignedBoundary(boolean rocks, boolean before)
      throws Exception {
    try (var reference = new GroupAggregateStockTestFixture("hidden", rocks, 0, before);
        var source = new GroupAggregateStockTestFixture("hidden", rocks, 0, before)) {
      reference.harness.open();
      source.harness.open();
      for (var fixture : List.of(reference, source)) {
        var backend = fixture.harness.getOperator().<RowData>getKeyedStateBackend();
        backend.setCurrentKey(
            backend.getKeySerializer().copy(fixture.selector.getKey(row(1L, null, 0))));
        ((ValueState<RowData>) fixture.rawState())
            .update(GenericRowData.of(Long.MIN_VALUE, -1L, Long.MIN_VALUE, -1L, 1L));
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
          assertState(reference, saved, "hidden", rocks, 0, before);
        }
      }
      try (var returned = new GroupAggregateStockTestFixture("hidden", rocks, 0, before)) {
        returned.harness.initializeState(saved);
        returned.harness.open();
        assertEquals(
            stockStep(reference, row(1L, 1L, 3), 1100), stockStep(returned, row(1L, 1L, 3), 1100));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }
}
