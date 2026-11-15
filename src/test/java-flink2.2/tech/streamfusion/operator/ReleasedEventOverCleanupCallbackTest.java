package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeUnboundedPrecedingOverFunctionV2;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

/** Compare actual released V2 timer callbacks; this is not a canonical adapter test. */
class ReleasedEventOverCleanupCallbackTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3), new BigIntType());

  static Stream<Arguments> modes() {
    var cases = new ArrayList<Arguments>();
    for (boolean stockStore : new boolean[] {false, true})
      for (boolean nativeStore : new boolean[] {false, true})
        for (boolean rows : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            cases.add(Arguments.of(stockStore, nativeStore, rows, keyless));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void releasedIdleAndPendingCleanupMatchesNativeCallbacks(
      boolean stockStore, boolean nativeStore, boolean rows, boolean keyless) throws Exception {
    try (var stock = stock(stockStore, rows, keyless);
        var nativeOp = NativeOverEventCleanupTimerTest.fixture(nativeStore, rows, keyless, false)) {
      stock.open();
      nativeOp.harness.open();
      stock.setProcessingTime(100);
      nativeOp.harness.setProcessingTime(100);
      push(stock, 1, 3, 1);
      nativeOp.push(1, 3, 1);
      stock.processWatermark(new Watermark(1));
      nativeOp.harness.processWatermark(new Watermark(1));
      assertEquals(drain(stock), nativeOp.drain());
      push(stock, 1, 7, 10);
      nativeOp.push(1, 7, 10);
      push(stock, 2, 5, 10);
      nativeOp.push(2, 5, 10);
      push(stock, 9, 99, 1);
      nativeOp.push(9, 99, 1);
      stock.setProcessingTime(1800);
      nativeOp.harness.setProcessingTime(1800);
      assertArrayEquals(new long[] {3300}, Native.nextOverEventCleanup(nativeOp.operator.handle));
      assertEquals(drain(stock), nativeOp.drain());
      stock.processWatermark(new Watermark(10));
      nativeOp.harness.processWatermark(new Watermark(10));
      assertEquals(sorted(drain(stock)), sorted(nativeOp.drain()));
      stock.setProcessingTime(3300);
      nativeOp.harness.setProcessingTime(3300);
      assertArrayEquals(new long[0], Native.nextOverEventCleanup(nativeOp.operator.handle));
      assertEquals(0, nativeOp.foldRows());
      push(stock, 1, 2, 11);
      nativeOp.push(1, 2, 11);
      stock.processWatermark(new Watermark(11));
      nativeOp.harness.processWatermark(new Watermark(11));
      var expected = drain(stock);
      assertEquals(List.of(2L), expected);
      assertEquals(expected, nativeOp.drain());
    }
  }

  private static List<Long> sorted(List<Long> values) {
    values.sort(Long::compare);
    return values;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      long key,
      long value,
      long timestamp)
      throws Exception {
    var row = GenericRowData.of(key, value, TimestampData.fromEpochMillis(timestamp));
    harness.processElement(
        new StreamRecord<>(new RowDataSerializer(INPUT).toBinaryRow(row).copy()));
  }

  private static List<Long> drain(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    var values = new ArrayList<Long>();
    for (var row : harness.extractOutputValues()) values.add(row.getLong(3));
    harness.getOutput().clear();
    return values;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      boolean rocks, boolean rows, boolean keyless) throws Exception {
    var generated =
        new GeneratedAggsHandleFunction("EventCleanupSum", "", new Object[0]) {
          @Override
          public AggsHandleFunction newInstance(ClassLoader loader) {
            return new Sum();
          }
        };
    var function =
        new RowTimeUnboundedPrecedingOverFunctionV2<RowData>(
            rows,
            1000,
            1500,
            generated,
            new LogicalType[] {new BigIntType()},
            INPUT.getChildren().toArray(new LogicalType[0]),
            2);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            ReleasedEventOverCleanupCallbackTest.class.getClassLoader(),
            keyless ? new int[0] : new int[] {0},
            InternalTypeInfo.of(INPUT));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new KeyedProcessOperator<>(function), selector, selector.getProducedType(), 128, 1, 0);
    var config = new Configuration();
    config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
    harness.setStateBackend(
        StateBackendLoader.loadStateBackendFromConfig(
            config, ReleasedEventOverCleanupCallbackTest.class.getClassLoader(), null));
    harness.setup(new RowDataSerializer(OUTPUT));
    return harness;
  }

  private static final class Sum implements AggsHandleFunction {
    private RowData state;

    public void open(StateDataViewStore store) {}

    public void accumulate(RowData row) {
      state = GenericRowData.of(state.getLong(0) + row.getLong(1));
    }

    public void retract(RowData row) {
      throw new UnsupportedOperationException();
    }

    public void merge(RowData other) {
      throw new UnsupportedOperationException();
    }

    public void setAccumulators(RowData state) {
      this.state = state;
    }

    public void resetAccumulators() {
      state = createAccumulators();
    }

    public RowData getAccumulators() {
      return state;
    }

    public RowData createAccumulators() {
      return GenericRowData.of(0L);
    }

    public RowData getValue() {
      return GenericRowData.of(state.getLong(0));
    }

    public void setWindowSize(int size) {}

    public void cleanup() {}

    public void close() {}
  }
}
