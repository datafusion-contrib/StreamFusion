package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupArithmeticEmptyStateTest {
  static Stream<Arguments> cases() {
    return Stream.of("arithmetic", "hidden", "count", "star")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                append ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            integer ->
                                                                Arguments.of(
                                                                    mode, rocks, ttl, append,
                                                                    integer))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void repeatedEmptyHandoffsDoNotCreateAnAccumulator(
      String mode, boolean rocks, long ttl, boolean append, boolean integer) throws Exception {
    try (var source = new GroupAggregateStockTestFixture(mode, rocks, ttl, true, append, integer);
        var reference =
            new GroupAggregateStockTestFixture(mode, rocks, ttl, true, append, integer)) {
      source.harness.open();
      reference.harness.open();
      assertTrue(source.rawStateBytes().isEmpty());
      var saved = snapshot(source.harness);
      try (var target = nativeHarness(source, rocks)) {
        target.initializeState(saved);
        target.open();
        ((DirectGroup) target.getOperator()).assertStorage();
        for (int round = 0; round < 2; round++) {
          saved = snapshot(target);
          assertState(reference, saved, mode, rocks, ttl, true, append);
          assertTrue(target.extractOutputValues().isEmpty());
        }
        Object key = integer ? (Object) Integer.MIN_VALUE : Long.MIN_VALUE;
        for (Long value : new Long[] {null, 7L}) {
          var row = GenericRowData.of(key, value);
          assertEquals(
              stockStep(reference, row, 1000),
              nativeStep(target, row, 1000, source.outputType, source.inputType));
          saved = snapshot(target);
          assertState(reference, saved, mode, rocks, ttl, true, append);
        }
      }
      try (var returned =
          new GroupAggregateStockTestFixture(mode, rocks, ttl, true, append, integer)) {
        returned.harness.initializeState(saved);
        returned.harness.open();
        var row = GenericRowData.of(null, 5L);
        assertEquals(stockStep(reference, row, 1100), stockStep(returned, row, 1100));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }
}
