package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class FlinkProctimeWindowBoundaryContractTest {
  @Test
  void stockAdmitsRowsAtTheClockOfAnAlreadyFiredTimer() throws Exception {
    try (var harness = harness()) {
      harness.setup(new RowDataSerializer(
          RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType())));
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(GenericRowData.of(1L, 0L)));
      harness.setProcessingTime(998);
      assertEquals(List.of(), rows(harness));
      harness.setProcessingTime(999);
      assertEquals(List.of(List.of(1L, 0L, 1000L)), rows(harness));
      harness.processElement(new StreamRecord<>(GenericRowData.of(2L, 0L)));
      // Flush Flink's aggregation bundle before its re-registered timer; the test pins
      // admission rather than the nondeterministic bundle/timer ordering of a live task.
      harness.prepareSnapshotPreBarrier(1);
      harness.setProcessingTime(1000);
      assertEquals(List.of(List.of(2L, 0L, 1000L)), rows(harness));
      harness.setProcessingTime(1001);
      assertEquals(List.of(), rows(harness));
    }
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness()
      throws Exception {
    Class<?> builders =
        compatibleClass(
            "org.apache.flink.table.runtime.operators.aggregate.window.WindowAggOperatorBuilder",
            "org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorBuilder");
    Object builder = builders.getMethod("builder").invoke(null);
    Class<?> assigners = compatibleClass(
        "org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners",
        "org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners");
    Object assigner =
        assigners
            .getMethod("tumbling", int.class, java.time.ZoneId.class, Duration.class)
            .invoke(null, -1, ZoneOffset.UTC, Duration.ofSeconds(1));
    configure(
        builder,
        "inputSerializer",
        new RowDataSerializer(RowType.of(new BigIntType(), new BigIntType())));
    configure(builder, "keySerializer", new BinaryRowDataSerializer(1));
    configure(builder, "shiftTimeZone", ZoneOffset.UTC);
    configure(builder, "assigner", assigner);
    var aggregate = new GeneratedNamespaceAggsHandleFunction<Long>("", "", new Object[0]) {
      @Override public NamespaceAggsHandleFunction<Long> newInstance(ClassLoader loader) {
        return new Sum();
      }
    };
    configure(builder, "aggregate", aggregate, new RowDataSerializer(RowType.of(new BigIntType())));
    var operator =
        (OneInputStreamOperator<RowData, RowData>) builders.getMethod("build").invoke(builder);
    RowType keyType = RowType.of(new BigIntType());
    var keySerializer = new RowDataSerializer(keyType);
    return new KeyedOneInputStreamOperatorTestHarness<>(operator,
        row -> keySerializer.toBinaryRow(GenericRowData.of(1L)).copy(), InternalTypeInfo.of(keyType));
  }

  private static Class<?> compatibleClass(String current, String legacy) throws Exception {
    try { return Class.forName(current); }
    catch (ClassNotFoundException absent) { return Class.forName(legacy); }
  }

  private static void configure(Object builder, String name, Object... arguments) throws Exception {
    for (var method : builder.getClass().getMethods()) {
      if (method.getName().equals(name) && method.getParameterCount() == arguments.length) {
        method.invoke(builder, arguments);
        return;
      }
    }
    throw new NoSuchMethodException(name);
  }

  private static List<List<Long>> rows(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    List<List<Long>> output = new ArrayList<>();
    for (Object event; (event = harness.getOutput().poll()) != null; ) {
      if (event instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) event);

        RowData row = (RowData) record.getValue();
        output.add(List.of(row.getLong(1), row.getLong(2), row.getLong(3)));
      }
    }
    return output;
  }

  private static final class Sum implements NamespaceAggsHandleFunction<Long> {
    private long sum;
    @Override public void open(StateDataViewStore views) {}
    @Override public void setAccumulators(Long namespace, RowData accumulators) { sum = accumulators.getLong(0); }
    @Override public void accumulate(RowData row) { sum += row.getLong(0); }
    @Override public void retract(RowData row) { sum -= row.getLong(0); }
    @Override public void merge(Long namespace, RowData accumulators) { sum += accumulators.getLong(0); }
    @Override public RowData createAccumulators() { return GenericRowData.of(0L); }
    @Override public RowData getAccumulators() { return GenericRowData.of(sum); }
    @Override public RowData getValue(Long end) { return GenericRowData.of(sum, end - 1000, end); }
    @Override public void cleanup(Long namespace) {}
    @Override public void close() {}
  }
}
