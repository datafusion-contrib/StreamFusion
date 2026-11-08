package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalUnboundedOverRecoveryTest {
  static Stream<Arguments> recoveries() {
    return Stream.of("BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            type ->
                emptyCases()
                    .map(
                        args ->
                            Arguments.of(
                                type,
                                args.get()[0],
                                args.get()[1],
                                args.get()[2],
                                args.get()[3],
                                args.get()[4])));
  }

  static Stream<Arguments> emptyCases() {
    return Stream.of(false, true)
        .flatMap(
            rows ->
                Stream.of(false, true)
                    .flatMap(
                        global ->
                            Stream.of(false, true)
                                .flatMap(
                                    source ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                target ->
                                                    Stream.of(0L, 1000L)
                                                        .map(
                                                            ttl ->
                                                                Arguments.of(
                                                                    rows, global, source, target,
                                                                    ttl))))));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void nativeCheckpointRecoveryThenCanonicalStoreChangePreservesExactStockState(
      String type, boolean rows, boolean global, boolean sourceStore, boolean targetStore, long ttl)
      throws Exception {
    var plan = new Plan(type, rows, global, ttl);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    var prefix = List.of(plan.row(1L, 7L), plan.row(null, null), plan.row(1L, 4L));
    var suffix = List.of(plan.row(1L, 3L), plan.row(null, 2L));
    try (var stock = plan.stock(sourceStore);
        var source = plan.nativeHarness(sourceStore)) {
      stock.open();
      source.open();
      time(stock, 100);
      time(source, 100);
      assertEquals(
          sourceStore, ((NativeOverAggregateOperator) source.getOperator()).directRocksDBState());
      assertEquals(plan.stockInput(stock, prefix), plan.nativeInput(source, prefix));
      var expected = raw(stock);
      var checkpoint = source.snapshot(33, 100);
      long continuedTime = ttl > 0 ? 1500 : 500;
      time(stock, continuedTime);
      time(source, continuedTime);
      var continued = plan.stockInput(stock, suffix);
      assertEquals(continued, plan.nativeInput(source, suffix));
      try (var recovered = plan.nativeHarness(sourceStore)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        try (var target = plan.nativeHarness(targetStore)) {
          target.initializeState(snapshot(recovered));
          target.open();
          assertEquals(
              targetStore,
              ((NativeOverAggregateOperator) target.getOperator()).directRocksDBState());
          try (var returned = plan.stock(targetStore)) {
            returned.initializeState(snapshot(target));
            returned.open();
            assertEquals(
                expected,
                raw(returned),
                "checkpoint recovery and canonical export preserve every raw scalar and age");
            time(returned, continuedTime);
            assertEquals(continued, plan.stockInput(returned, suffix));
          }
          time(target, continuedTime);
          assertEquals(continued, plan.nativeInput(target, suffix));
        }
        time(recovered, continuedTime);
        assertEquals(continued, plan.nativeInput(recovered, suffix));
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("emptyCases")
  void emptyRoundTripDoesNotInventAccumulators(
      boolean rows, boolean global, boolean stockStore, boolean nativeStore, long ttl)
      throws Exception {
    var plan = new Plan("BIGINT", rows, global, ttl);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = plan.stock(stockStore);
        var target = plan.nativeHarness(nativeStore)) {
      stock.open();
      assertTrue(raw(stock).isEmpty());
      target.initializeState(snapshot(stock));
      target.open();
      try (var returned = plan.stock(stockStore)) {
        returned.initializeState(snapshot(target));
        returned.open();
        assertTrue(raw(returned).isEmpty());
      }
      time(stock, 100);
      time(target, 100);
      var input = List.of(plan.row(1L, null), plan.row(null, 7L));
      assertEquals(plan.stockInput(stock, input), plan.nativeInput(target, input));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }
}
