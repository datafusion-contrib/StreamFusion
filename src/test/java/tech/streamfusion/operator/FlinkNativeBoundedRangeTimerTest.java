package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FlinkNativeBoundedPrimitiveArithmeticTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Released timer behavior and ordinary recovery; cross-engine canonical restore is separate. */
class FlinkNativeBoundedRangeTimerTest {
  @BeforeAll
  static void memory() {
    memoryAuthority();
  }

  static Stream<Arguments> modes() {
    return Stream.of(false, true)
        .flatMap(rocks -> Stream.of(0, 1, 2).map(phase -> Arguments.of(rocks, phase)));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void supersededCleanupTimerAdvancesTheLateCutoff(boolean rocks, int restorePhase)
      throws Exception {
    var type = new BigIntType();
    var input = RowType.of(type, type);
    var output = RowType.of(type, type, type, type, type, type);
    OperatorSubtaskState stockSnapshot = null, nativeSnapshot = null;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 2, rocks, false, 32);
        var nativeOperator = nativeHarness(0, input, 2, rocks, false, 32)) {
      stock.setup(new org.apache.flink.table.runtime.typeutils.RowDataSerializer(output));
      nativeOperator.setup(new ArrowBatchSerializer());
      stock.open();
      nativeOperator.open();
      assertEquals(
          stepStock(stock, row(0, 1L, 1000), 2, output),
          stepNative(nativeOperator, row(0, 1L, 1000), 2, input, allocator, output));
      stock.getOutput().clear();
      stock.processElement(new StreamRecord<>(row(0, 7L, 1064)));
      nativeOperator.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row(0, 7L, 1064)), input, allocator))));
      if (restorePhase != 1) {
        stock.processWatermark(new Watermark(1050));
        nativeOperator.processWatermark(new Watermark(1050));
        assertTrue(stock.extractOutputValues().isEmpty());
        assertTrue(nativeOperator.extractOutputValues().isEmpty());
      }
      if (restorePhase == 0) {
        finish(stock, nativeOperator, input, output, allocator);
      } else {
        stockSnapshot = stock.snapshot(1, 1);
        nativeSnapshot = nativeOperator.snapshot(1, 1);
      }
    }
    if (restorePhase != 0)
      try (var allocator = new RootAllocator();
          var stock = stock(type, input, 2, rocks, false, 32);
          var nativeOperator = nativeHarness(0, input, 2, rocks, false, 32)) {
        stock.setup(new org.apache.flink.table.runtime.typeutils.RowDataSerializer(output));
        nativeOperator.setup(new ArrowBatchSerializer());
        stock.initializeState(stockSnapshot);
        nativeOperator.initializeState(nativeSnapshot);
        stock.open();
        nativeOperator.open();
        if (restorePhase == 1) {
          stock.processWatermark(new Watermark(1050));
          nativeOperator.processWatermark(new Watermark(1050));
        }
        finish(stock, nativeOperator, input, output, allocator);
      }
  }

  static void finish(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      RowType input,
      RowType output,
      RootAllocator allocator)
      throws Exception {
    stock.getOutput().clear();
    stock.processElement(new StreamRecord<>(row(0, 99L, 1020)));
    nativeOperator.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(List.of(row(0, 99L, 1020)), input, allocator))));
    stock.processWatermark(new Watermark(1064));
    nativeOperator.processWatermark(new Watermark(1064));
    assertEquals(List.of("7:7:1:7"), values(stock.extractOutputValues(), output));
    var actual = new java.util.ArrayList<String>();
    for (var batch : nativeOperator.extractOutputValues())
      try (var root = batch.root()) {
        actual.addAll(values(RowDataArrowConverter.read(root, output), output));
      }
    nativeOperator.getOutput().clear();
    assertEquals(List.of("7:7:1:7"), actual);
  }
}
