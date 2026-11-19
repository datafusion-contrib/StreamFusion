package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FlinkNativeBoundedPrimitiveArithmeticTest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Released event-time ROWS peers, terminal timestamp and native checkpoint continuation. */
class FlinkNativeEventRowsProtocolTest {
  @BeforeAll
  static void memory() {
    memoryAuthority();
  }

  static Stream<Arguments> modes() {
    var cases = new ArrayList<Arguments>();
    for (int code : new int[] {0, 6, 1})
      for (boolean nonfinite : code == 0 ? new boolean[] {false} : new boolean[] {false, true})
        for (int offset : new int[] {0, 2})
          for (boolean rocks : new boolean[] {false, true})
            for (boolean distinct : new boolean[] {false, true})
              for (int time = 0; time < 4; time++)
                for (boolean firePrefix : new boolean[] {false, true})
                  cases.add(
                      Arguments.of(code, nonfinite, offset, rocks, distinct, time, firePrefix));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void maximumTimestampPeersContinueAfterOrdinaryNativeRecovery(
      int code,
      boolean nonfinite,
      int offset,
      boolean rocks,
      boolean distinct,
      int time,
      boolean firePrefix)
      throws Exception {
    var type = type(code);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, new BigIntType(), type);
    long first = time == 0 ? 100 : time == 1 ? Long.MAX_VALUE - 1 : Long.MAX_VALUE;
    long last = time == 0 ? 200 : Long.MAX_VALUE;
    long[] times =
        time == 3
            ? new long[] {200, 100, 300, 100, 200, 300}
            : new long[] {first, first, last, last, last, last};
    long prefixWatermark = Math.max(times[0], times[1]);
    long finalWatermark = java.util.Arrays.stream(times).max().orElseThrow();
    Number[] data =
        nonfinite
            ? new Number[] {Double.POSITIVE_INFINITY, 2L, Double.NEGATIVE_INFINITY, 4L, null, 2L}
            : new Number[] {1L, 2L, 3L, 4L, null, 2L};
    OperatorSubtaskState stockSnapshot, nativeSnapshot;
    List<String> expected;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 1, rocks, distinct, offset);
        var nativeOp = nativeHarness(code, input, 1, rocks, distinct, offset)) {
      stock.setup(new RowDataSerializer(output));
      nativeOp.setup(new ArrowBatchSerializer());
      stock.open();
      nativeOp.open();
      for (int i = 0; i < 2; i++) {
        stock.processElement(new StreamRecord<>(row(code, data[i], times[i])));
        push(nativeOp, row(code, data[i], times[i]), input, allocator);
      }
      if (firePrefix) {
        stock.processWatermark(new Watermark(prefixWatermark));
        nativeOp.processWatermark(new Watermark(prefixWatermark));
        assertEquals(values(stock.extractOutputValues(), output), drain(nativeOp, output));
        stock.getOutput().clear();
      }
      stockSnapshot = stock.snapshot(1, 1);
      nativeSnapshot = nativeOp.snapshot(1, 1);
      for (int i = 2; i < data.length; i++) {
        stock.processElement(new StreamRecord<>(row(code, data[i], times[i])));
        push(nativeOp, row(code, data[i], times[i]), input, allocator);
      }
      stock.processWatermark(new Watermark(finalWatermark));
      nativeOp.processWatermark(new Watermark(finalWatermark));
      expected = values(stock.extractOutputValues(), output);
      assertEquals(expected, drain(nativeOp, output));
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 1, rocks, distinct, offset);
        var nativeOp = nativeHarness(code, input, 1, rocks, distinct, offset)) {
      stock.setup(new RowDataSerializer(output));
      nativeOp.setup(new ArrowBatchSerializer());
      stock.initializeState(stockSnapshot);
      nativeOp.initializeState(nativeSnapshot);
      stock.open();
      nativeOp.open();
      for (int i = 2; i < data.length; i++) {
        stock.processElement(new StreamRecord<>(row(code, data[i], times[i])));
        push(nativeOp, row(code, data[i], times[i]), input, allocator);
      }
      stock.processWatermark(new Watermark(finalWatermark));
      nativeOp.processWatermark(new Watermark(finalWatermark));
      assertEquals(expected, values(stock.extractOutputValues(), output));
      assertEquals(expected, drain(nativeOp, output));
    }
  }

  static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowData row,
      RowType input,
      RootAllocator allocator)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
  }

  static List<String> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType output) {
    var rows = new ArrayList<String>();
    for (var batch : harness.extractOutputValues())
      try (var root = batch.root()) {
        rows.addAll(values(RowDataArrowConverter.read(root, output), output));
      }
    harness.getOutput().clear();
    return rows;
  }
}
