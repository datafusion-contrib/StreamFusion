package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;

class NativeJvmAggregateTtlTest {
  private static final RowType TYPE = RowType.of(new IntType(), new BigIntType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void releasedTtlPreservesNoOpUpdatesExpiryAndRestoredClocks(boolean restore) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(TYPE));
    var nativeHarness = nativeHarness();
    try (var allocator = new RootAllocator();
        var host =
            new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                new KeyedProcessOperator<>(function()), selector, selector.getProducedType())) {
      host.setup(new RowDataSerializer(TYPE));
      host.open();
      nativeHarness.open();
      List<String> expected = new ArrayList<>();
      List<String> actual = new ArrayList<>();
      long[] times = {1000, 1500, 2400, 3400, 4500, 4500, 4500};
      long[] values = {5, 0, 2, 3, 3, 1, 2};
      for (int index = 0; index < times.length; index++) {
        host.setStateTtlProcessingTime(times[index]);
        nativeHarness.setStateTtlProcessingTime(times[index]);
        RowData row = GenericRowData.of(1, values[index]);
        if (index == 4) row.setRowKind(RowKind.DELETE);
        host.processElement(new StreamRecord<>(row));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), TYPE, allocator, true))));
        for (RowData output : host.extractOutputValues()) expected.add(value(output));
        host.getOutput().clear();
        for (ArrowBatch batch : nativeHarness.extractOutputValues()) {
          try (var root = batch.root()) {
            var reader = ArrowConversion.createArrowReader(root, TYPE);
            var kinds =
                (org.apache.arrow.vector.TinyIntVector)
                    root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
            for (int i = 0; i < root.getRowCount(); i++) {
              var output = reader.read(i);
              output.setRowKind(RowKind.fromByteValue(kinds.get(i)));
              actual.add(value(output));
            }
          }
        }
        nativeHarness.getOutput().clear();
        assertEquals(expected, actual, "after TTL time " + times[index]);
        if (restore && index == 2) {
          var state = nativeHarness.snapshot(1, times[index]);
          nativeHarness.close();
          nativeHarness = nativeHarness();
          nativeHarness.initializeState(state);
          nativeHarness.open();
        }
      }
      assertEquals(
          List.of(
              "+I|1|5", "-U|1|5", "+U|1|5", "-U|1|5", "+U|1|7", "+I|1|3", "+I|1|1", "-U|1|1",
              "+U|1|3"),
          expected);
    } finally {
      nativeHarness.close();
    }
  }

  private static String value(RowData row) {
    return row.getRowKind().shortString() + "|" + row.getInt(0) + "|" + row.getLong(1);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch>
      nativeHarness() throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            Thread.currentThread().getContextClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(TYPE));
    var operator =
        new NativeColumnarJvmGroupOperator(
            TYPE, TYPE, function(), selector, 7, 3, LogicalTypeRoot.INTEGER, 0, 1000);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch>(
            operator, new ArrowBatchRowKeySelector(TYPE, selector), selector.getProducedType());
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static GroupAggFunction function() {
    var handler =
        new GeneratedAggsHandleFunction("TtlSum", "", new Object[0]) {
          @Override
          public AggsHandleFunction newInstance(ClassLoader loader) {
            return new SumHandler();
          }
        };
    var equaliser =
        new GeneratedRecordEqualiser("TtlEqualiser", "", new Object[0]) {
          @Override
          public RecordEqualiser newInstance(ClassLoader loader) {
            return (left, right) -> left.getLong(0) == right.getLong(0);
          }
        };
    return new GroupAggFunction(
        handler, equaliser, new LogicalType[] {new BigIntType(), new BigIntType()}, 1, true, 1000);
  }

  private static final class SumHandler implements AggsHandleFunction {
    private RowData state;

    public void open(StateDataViewStore store) {}

    public void accumulate(RowData row) {
      state = GenericRowData.of(state.getLong(0) + row.getLong(1), state.getLong(1) + 1);
    }

    public void retract(RowData row) {
      state = GenericRowData.of(state.getLong(0) - row.getLong(1), state.getLong(1) - 1);
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
      return GenericRowData.of(0L, 0L);
    }

    public RowData getValue() {
      return GenericRowData.of(state.getLong(0));
    }

    public void setWindowSize(int size) {}

    public void cleanup() {}

    public void close() {}
  }
}
