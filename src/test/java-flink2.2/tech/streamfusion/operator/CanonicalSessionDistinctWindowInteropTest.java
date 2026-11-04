package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalSharedWindowInteropTest.BASE;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.operator.CanonicalSharedWindowInteropTest.Input;
import tech.streamfusion.operator.CanonicalSharedWindowInteropTest.StockPlan;

class CanonicalSessionDistinctWindowInteropTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
      for (boolean stockRocks : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          cases.add(Arguments.of(zone, stockRocks, keyless));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void releasedSqlDistinctSessionsReexportWithoutInputThenMergeLateRows(
      String zone, boolean stockRocks, boolean keyless) throws Exception {
    var plan = new StockPlan("SESSION", zone, keyless, false, true);
    long handle = plan.create(false, directory);
    try (var source = plan.stock(stockRocks);
        var reference = plan.stock(stockRocks);
        var view = plan.harness(new Projection(), !stockRocks)) {
      source.open();
      for (var row : prefix()) plan.send(source, row);
      source.processWatermark(new Watermark(BASE + 1500));
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(snapshot);
      reference.open();
      view.initializeState(snapshot);
      view.open();
      var contract = plan.contract();
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      contract.importState(backend(view), handle, false);
      contract.snapshotOperatorState(BASE + 1500);
      contract.exportState(backend(view), handle, false);
      var immediate =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.stock(stockRocks)) {
        restored.initializeState(immediate);
        restored.open();
        var suffix =
            List.of(
                new Input(1L, 10L, 100),
                new Input(1L, 30L, 3000),
                new Input(1L, 40L, 5000),
                new Input(1L, 10L, 4000),
                new Input(null, null, 10500));
        for (var row : suffix) {
          plan.send(reference, row);
          plan.send(restored, row);
        }
        plan.update(handle, suffix);
        for (long watermark : new long[] {BASE + 3999, BASE + 5999, Long.MAX_VALUE}) {
          reference.processWatermark(new Watermark(watermark));
          restored.processWatermark(new Watermark(watermark));
          var expected = plan.output(reference);
          assertEquals(expected, plan.output(restored));
          assertEquals(expected, plan.flush(handle, watermark));
        }
      }
    } finally {
      Native.closeSessionAggregator(handle);
    }
  }

  static Stream<Arguments> corruptions() {
    return Stream.of("orphan", "bits", "zeroMembership", "cardinality", "missingMember")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void corruptSessionViewsCannotBecomeAUsableNativeRestore(String mode, boolean rocks)
      throws Exception {
    var plan = new StockPlan("SESSION", "UTC", false, false, true);
    long handle = plan.create(false, directory);
    try (var source = plan.stock(rocks);
        var view = plan.harness(new Projection(), !rocks)) {
      source.open();
      for (var row : prefix()) plan.send(source, row);
      source.processWatermark(new Watermark(BASE + 1500));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState());
      view.open();
      var backend = backend(view);
      var descriptor = plan.viewDescriptors.get(0);
      org.apache.flink.api.java.tuple.Tuple2<RowData, TimeWindow> entry;
      try (var entries = backend.<TimeWindow>getKeysAndNamespaces(descriptor.getName())) {
        entry = entries.findFirst().orElseThrow();
      }
      backend.setCurrentKey(entry.f0);
      var members = backend.getPartitionedState(entry.f1, new TimeWindow.Serializer(), descriptor);
      var member = members.entries().iterator().next();
      if (mode.equals("orphan"))
        backend
            .getPartitionedState(
                new TimeWindow(BASE + 20000, BASE + 21000), new TimeWindow.Serializer(), descriptor)
            .put(member.getKey(), member.getValue());
      else if (mode.equals("bits")) members.put(member.getKey(), 2L);
      else if (mode.equals("zeroMembership")) members.put(member.getKey(), 0L);
      else if (mode.equals("missingMember")) members.remove(member.getKey());
      else {
        assertEquals(7, plan.kinds[0]);
        var accumulators =
            backend.getPartitionedState(
                entry.f1,
                new TimeWindow.Serializer(),
                new ValueStateDescriptor<RowData>(
                    "window-aggs", new RowDataSerializer(plan.accumulators)));
        var previous = accumulators.value();
        var changed = new GenericRowData(plan.accumulators.getFieldCount());
        for (int i = 0; i < changed.getArity(); i++)
          changed.setField(
              i,
              RowData.createFieldGetter(plan.accumulators.getTypeAt(i), i)
                  .getFieldOrNull(previous));
        changed.setField(0, previous.getLong(0) + 1);
        accumulators.update(changed);
      }
      var damaged =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.harness(new Projection(), rocks)) {
        restored.initializeState(damaged);
        restored.open();
        var contract = plan.contract();
        contract.initializeOperatorState(((Projection) restored.getOperator()).store, true);
        assertThrows(
            IllegalStateException.class,
            () -> contract.importState(backend(restored), handle, false));
      }
      var rows = new AtomicInteger();
      Native.BatchReceiver receiver =
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              rows.addAndGet(root.getRowCount());
            }
          };
      if (mode.equals("cardinality")
          || mode.equals("missingMember")
          || mode.equals("zeroMembership"))
        assertThrows(
            IllegalStateException.class,
            () -> Native.exportSessionAggregatorCanonical(handle, receiver));
      else {
        Native.exportSessionAggregatorCanonical(handle, receiver);
        assertEquals(0, rows.get());
      }
    } finally {
      Native.closeSessionAggregator(handle);
    }
  }

  private static List<Input> prefix() {
    return List.of(
        new Input(1L, 10L, 0),
        new Input(1L, 20L, 2000),
        new Input(1L, 10L, 1000),
        new Input(null, null, 10000));
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view) {
    return (CheckpointableKeyedStateBackend<RowData>)
        view.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    transient org.apache.flink.api.common.state.OperatorStateStore store;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
      super.initializeState(context);
      store = context.getOperatorStateStore();
    }

    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
