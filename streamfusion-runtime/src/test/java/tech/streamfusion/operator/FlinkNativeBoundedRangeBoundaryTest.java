package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FlinkNativeBoundedPrimitiveArithmeticTest.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

/** Actual released callbacks, including wraparound and event timestamp emission order. */
class FlinkNativeBoundedRangeBoundaryTest {
  @BeforeAll
  static void memory() {
    memoryAuthority();
  }

  static Stream<Arguments> modes() {
    return Stream.of(false, true)
        .flatMap(store -> Stream.of(0, 1, 2, 3).map(scenario -> Arguments.of(store, scenario)));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void nativeWatermarksFollowTheReleasedTimerOrder(boolean store, int scenario) throws Exception {
    var type = new BigIntType();
    var input = RowType.of(type, type);
    var output = RowType.of(type, type, type, type, type, type);
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 2, store, false, 32);
        var nativeOp = nativeHarness(0, input, 2, store, false, 32)) {
      stock.setup(new RowDataSerializer(output));
      nativeOp.setup(new ArrowBatchSerializer());
      stock.open();
      nativeOp.open();
      long[] times;
      switch (scenario) {
        case 0:
          times = new long[] {1020, 1000, 1010};
          break;
        case 1:
          times = new long[] {Long.MAX_VALUE - 10};
          break;
        case 2:
          times = new long[] {Long.MAX_VALUE - 48};
          break;
        default:
          times = new long[] {Long.MAX_VALUE - 49, Long.MAX_VALUE};
      }
      for (long time : times) {
        var row = row(0, time % 100, time);
        stock.processElement(new StreamRecord<>(row));
        nativeOp.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
      }
      long watermark = scenario == 0 ? 1020 : Long.MAX_VALUE;
      stock.processWatermark(new Watermark(watermark));
      nativeOp.processWatermark(new Watermark(watermark));
      var expected = values(stock.extractOutputValues(), output);
      var actual = new ArrayList<String>();
      for (var batch : nativeOp.extractOutputValues())
        try (var root = batch.root()) {
          actual.addAll(values(RowDataArrowConverter.read(root, output), output));
        }
      nativeOp.getOutput().clear();
      assertEquals(scenario == 0 ? 3 : scenario == 3 ? 1 : 0, expected.size());
      assertEquals(expected, actual);
    }
  }
}
