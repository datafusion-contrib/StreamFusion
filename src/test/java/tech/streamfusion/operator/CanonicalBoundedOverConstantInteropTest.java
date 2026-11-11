package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalBoundedOverConstantInteropTest {
  static Stream<Arguments> cases() {
    return Stream.of("range", "range-zero", "rows-current", "rows-preceding")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        keyless ->
                            Stream.of(false, true)
                                .flatMap(
                                    stock ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                nativeStore ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            pending ->
                                                                Arguments.of(
                                                                    mode,
                                                                    keyless,
                                                                    stock,
                                                                    nativeStore,
                                                                    pending))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void explicitCountStarAndLiteralSumsKeepPhysicalArgumentsAcrossBothDirections(
      String mode, boolean keyless, boolean stockStore, boolean nativeStore, boolean pending)
      throws Exception {
    boolean rows = mode.startsWith("rows");
    int offset = rows ? mode.equals("rows-current") ? 0 : 2 : mode.equals("range-zero") ? 0 : 32;
    var plan =
        new CanonicalBoundedRangeInteropTest.Plan("BIGINT", keyless, false, rows, offset, 0, true);
    assertEquals(-1, plan.hiddenCount, "COUNT(*) already owns the input count");
    assertArrayEquals(
        new int[] {3, 3, 22, 3, 22}, plan.kinds, java.util.Arrays.toString(plan.kinds));
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState saved;
    List<String> expected;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      plan.prefix(stock, pending);
      saved = snapshot(stock);
      plan.suffix(stock);
      expected = plan.stockOutput(stock);
      assertFalse(expected.isEmpty());
    }
    OperatorSubtaskState returned;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(saved);
      nativeOp.open();
      returned = snapshot(nativeOp);
      plan.suffixNative(nativeOp);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(returned);
      stock.open();
      plan.suffix(stock);
      assertEquals(expected, plan.stockOutput(stock));
    }
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      plan.prefixNative(nativeOp, pending);
      saved = snapshot(nativeOp);
      plan.suffixNative(nativeOp);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(saved);
      stock.open();
      plan.suffix(stock);
      assertEquals(expected, plan.stockOutput(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    return harness
        .snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }
}
