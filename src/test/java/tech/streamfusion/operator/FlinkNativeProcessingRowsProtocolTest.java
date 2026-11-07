package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FlinkNativeBoundedPrimitiveArithmeticTest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Released processing-time bucket/minimum/counter behavior, including native recovery. */
class FlinkNativeProcessingRowsProtocolTest {
  @BeforeAll
  static void memory() {
    memoryAuthority();
  }

  static Stream<Arguments> modes() {
    var cases = new ArrayList<Arguments>();
    for (int offset : new int[] {0, 2})
      for (boolean rocks : new boolean[] {false, true})
        for (boolean distinct : new boolean[] {false, true})
          for (int clock = 0; clock < 4; clock++)
            for (int snapshotAt = 1; snapshotAt < 6; snapshotAt++)
              cases.add(Arguments.of(offset, rocks, distinct, clock, snapshotAt));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void releasedBucketProtocolContinuesThroughNativeCheckpoints(
      int offset, boolean rocks, boolean distinct, int clock, int snapshotAt) throws Exception {
    var type = new BigIntType();
    var input = RowType.of(type, type);
    var output = RowType.of(type, type, type, type, type, type);
    long[] times;
    switch (clock) {
      case 0:
        times = new long[] {100, 100, 100, 100, 100, 100};
        break;
      case 1:
        times = new long[] {100, 200, 300, 400, 500, 600};
        break;
      case 2:
        times =
            new long[] {
              Long.MAX_VALUE,
              Long.MAX_VALUE,
              Long.MAX_VALUE,
              Long.MAX_VALUE,
              Long.MAX_VALUE,
              Long.MAX_VALUE
            };
        break;
      default:
        times = new long[] {300, 200, 100, 400, 200, 500};
    }
    Number[] data = {1L, 2L, 3L, 4L, null, 2L};
    OperatorSubtaskState stockSnapshot, nativeSnapshot;
    var expectedSuffix = new ArrayList<List<String>>();
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 0, rocks, distinct, offset);
        var nativeOp = nativeHarness(0, input, 0, rocks, distinct, offset)) {
      stock.setup(new RowDataSerializer(output));
      nativeOp.setup(new ArrowBatchSerializer());
      stock.open();
      nativeOp.open();
      for (int i = 0; i < snapshotAt; i++)
        assertEquals(
            stepStock(stock, row(0, data[i], times[i]), 0, output),
            stepNative(nativeOp, row(0, data[i], times[i]), 0, input, allocator, output));
      stockSnapshot = stock.snapshot(1, 1);
      nativeSnapshot = nativeOp.snapshot(1, 1);
      for (int i = snapshotAt; i < data.length; i++) {
        var expected = stepStock(stock, row(0, data[i], times[i]), 0, output);
        expectedSuffix.add(expected);
        assertEquals(
            expected, stepNative(nativeOp, row(0, data[i], times[i]), 0, input, allocator, output));
      }
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 0, rocks, distinct, offset);
        var nativeOp = nativeHarness(0, input, 0, rocks, distinct, offset)) {
      stock.setup(new RowDataSerializer(output));
      nativeOp.setup(new ArrowBatchSerializer());
      stock.initializeState(stockSnapshot);
      nativeOp.initializeState(nativeSnapshot);
      stock.open();
      nativeOp.open();
      for (int i = snapshotAt; i < data.length; i++) {
        var expected = stepStock(stock, row(0, data[i], times[i]), 0, output);
        assertEquals(expectedSuffix.get(i - snapshotAt), expected);
        assertEquals(
            expected, stepNative(nativeOp, row(0, data[i], times[i]), 0, input, allocator, output));
      }
    }
  }
}
