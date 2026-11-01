package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
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

class NativeJvmMiniBatchCheckpointTest {
  private static final RowType TYPE = RowType.of(new IntType(), new BigIntType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ownedPendingRowsFlushAtBarriersWatermarksAndFinishWithReleasedTtl(boolean restore)
      throws Exception {
    var host = host();
    var nativeHarness = nativeHarness();
    try (var allocator = new RootAllocator()) {
      host.open();
      nativeHarness.open();
      var expected = new ArrayList<String>();
      var actual = new ArrayList<String>();
      long[] clocks = {1000, 1000, 1500, 1800, 2300, 2400, 3000, 3500, 3600, 4700};
      for (int i = 0; i < clocks.length; i++) {
        host.setStateTtlProcessingTime(clocks[i]);
        nativeHarness.setStateTtlProcessingTime(clocks[i]);
        var row = GenericRowData.of(i % 3 == 0 ? null : i % 2 + 1, (long) i + 1);
        if (i == 7) row.setRowKind(RowKind.DELETE);
        host.processElement(new StreamRecord<>(row));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), TYPE, allocator, true))));
        row.setField(1, 999999L);
        drain(host, nativeHarness, expected, actual);
        assertEquals(expected, actual, "after input " + i);
        if (i == 4) {
          host.prepareSnapshotPreBarrier(7);
          nativeHarness.prepareSnapshotPreBarrier(7);
          drain(host, nativeHarness, expected, actual);
          assertEquals(expected, actual, "pending checkpoint bundle");
          var hostState = host.snapshot(7, clocks[i]);
          var nativeState = nativeHarness.snapshot(7, clocks[i]);
          if (restore) {
            host.close();
            nativeHarness.close();
            host = host();
            nativeHarness = nativeHarness();
            host.initializeState(hostState);
            nativeHarness.initializeState(nativeState);
            host.open();
            nativeHarness.open();
          }
        }
        if (i == 6) {
          host.processWatermark(new Watermark(42));
          nativeHarness.processWatermark(new Watermark(42));
          assertEquals(
              host.getOutput().stream()
                  .filter(v -> v instanceof Watermark)
                  .collect(tech.streamfusion.compat.ListCollectors.toList()),
              nativeHarness.getOutput().stream()
                  .filter(v -> v instanceof Watermark)
                  .collect(tech.streamfusion.compat.ListCollectors.toList()));
          drain(host, nativeHarness, expected, actual);
          assertEquals(expected, actual, "watermark bundle");
        }
      }
      host.getOperator().finish();
      nativeHarness.getOperator().finish();
      drain(host, nativeHarness, expected, actual);
      assertEquals(expected, actual, "finished bundle");
      assertFalse(expected.isEmpty());
    } finally {
      host.close();
      nativeHarness.close();
    }
  }

  private static void drain(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host,
      KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch> nativeHarness,
      List<String> expected,
      List<String> actual) {
    for (var row : host.extractOutputValues()) expected.add(value(row));
    host.getOutput().clear();
    for (var batch : nativeHarness.extractOutputValues())
      try (var root = batch.root()) {
        var reader = ArrowConversion.createArrowReader(root, TYPE);
        var kinds =
            (org.apache.arrow.vector.TinyIntVector)
                root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        for (int i = 0; i < root.getRowCount(); i++) {
          var row = reader.read(i);
          row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
          actual.add(value(row));
        }
      }
    nativeHarness.getOutput().clear();
  }

  private static String value(RowData row) {
    return row.getRowKind().shortString()
        + "|"
        + (row.isNullAt(0) ? "null" : row.getInt(0))
        + "|"
        + row.getLong(1);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host()
      throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            Thread.currentThread().getContextClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(TYPE));
    var h =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new KeyedMapBundleOperator<>(function(), new CountBundleTrigger<>(3)),
            selector,
            selector.getProducedType());
    h.setup(new RowDataSerializer(TYPE));
    return h;
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
            TYPE,
            TYPE,
            function(),
            new CountBundleTrigger<>(3),
            selector,
            7,
            LogicalTypeRoot.INTEGER,
            0,
            1000);
    var h =
        new KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch>(
            operator, new ArrowBatchRowKeySelector(TYPE, selector), selector.getProducedType());
    h.setup(new ArrowBatchSerializer());
    return h;
  }

  private static MiniBatchGroupAggFunction function() {
    var handler =
        new GeneratedAggsHandleFunction("MiniSum", "", new Object[0]) {
          @Override
          public AggsHandleFunction newInstance(ClassLoader loader) {
            return new NativeJvmAggregateTtlTest.SumHandler();
          }
        };
    var equaliser =
        new GeneratedRecordEqualiser("MiniEqualiser", "", new Object[0]) {
          @Override
          public RecordEqualiser newInstance(ClassLoader loader) {
            return (a, b) -> a.getLong(0) == b.getLong(0);
          }
        };
    return new MiniBatchGroupAggFunction(
        handler,
        equaliser,
        new LogicalType[] {new BigIntType(), new BigIntType()},
        TYPE,
        1,
        true,
        1000);
  }
}
