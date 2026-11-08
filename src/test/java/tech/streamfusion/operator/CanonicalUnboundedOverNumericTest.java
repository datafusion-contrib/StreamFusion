package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalUnboundedOverNumericTest {
  static Stream<Arguments> cases() {
    return Stream.of("BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rows ->
                            Stream.of(false, true)
                                .flatMap(
                                    global ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                stock ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            nativeStore ->
                                                                Arguments.of(
                                                                    type,
                                                                    rows,
                                                                    global,
                                                                    stock,
                                                                    nativeStore))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void overflowSignedZeroInfinitiesAndNanSurviveBothDirections(
      String type, boolean rows, boolean global, boolean stockStore, boolean nativeStore)
      throws Exception {
    var plan = new Plan(type, rows, global, 0);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    List<?> values;
    if (type.equals("BIGINT"))
      values = Arrays.asList(Long.MAX_VALUE, 1L, Long.MIN_VALUE, -1L, null, 1L);
    else if (type.equals("FLOAT"))
      values =
          Arrays.asList(
              -0F,
              Float.MIN_VALUE,
              Float.MAX_VALUE,
              Float.POSITIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              Float.intBitsToFloat(0x7fc00011),
              null,
              0F);
    else
      values =
          Arrays.asList(
              -0D,
              Double.MIN_VALUE,
              Double.MAX_VALUE,
              Double.POSITIVE_INFINITY,
              Double.NEGATIVE_INFINITY,
              Double.longBitsToDouble(0x7ff8000000000011L),
              null,
              0D);
    var input = new ArrayList<RowData>();
    for (Object number : values) {
      var row = (GenericRowData) plan.row(1L, null);
      row.setField(1, number);
      input.add(row);
    }
    var prefix = new ArrayList<>(input.subList(0, 3));
    prefix.add(plan.row(null, null));
    var suffix = input.subList(3, input.size());
    try (var stock = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      stock.open();
      nativeOp.open();
      time(stock, 100);
      time(nativeOp, 100);
      assertEquals(plan.stockInput(stock, prefix), plan.nativeInput(nativeOp, prefix));
      var stockState = snapshot(stock);
      var nativeState = snapshot(nativeOp);
      try (var returned = plan.stock(stockStore);
          var restored = plan.nativeHarness(nativeStore)) {
        returned.initializeState(nativeState);
        returned.open();
        restored.initializeState(stockState);
        restored.open();
        assertEquals(raw(stock), raw(returned));
        time(stock, 500);
        time(nativeOp, 500);
        time(returned, 500);
        time(restored, 500);
        var expected = plan.stockInput(stock, suffix);
        assertEquals(expected, plan.stockInput(returned, suffix));
        assertEquals(expected, plan.nativeInput(nativeOp, suffix));
        assertEquals(expected, plan.nativeInput(restored, suffix));
        try (var finalStock = plan.stock(stockStore)) {
          finalStock.initializeState(snapshot(restored));
          finalStock.open();
          assertEquals(raw(stock), raw(finalStock));
        }
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }
}
