package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.over.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.arrow.ArrowConversion;

class NativeJvmOverTimerTest {
  private static final RowType INPUT =
      RowType.of(new IntType(), new BigIntType(), new TimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(new IntType(), new BigIntType(), new TimestampType(3), new BigIntType());

  @ParameterizedTest
  @CsvSource({
    "0,false", "0,true", "1,false", "1,true", "2,false", "2,true", "3,false", "3,true", "4,false",
    "4,true", "5,false", "5,true"
  })
  void controlledClocksRetentionBarriersAndRestoredTimersMatchReleasedFrames(
      int frame, boolean restore) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(INPUT));
    var host =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new KeyedProcessOperator<>(function(frame)), selector, selector.getProducedType());
    var nativeHarness = nativeHarness(frame);
    try (var allocator = new RootAllocator()) {
      host.setup(new RowDataSerializer(OUTPUT));
      host.open();
      nativeHarness.open();
      var serializer = new RowDataSerializer(INPUT);
      List<String> expected = new ArrayList<>(), actual = new ArrayList<>();
      long[] clocks = {1000, 1100, 1200, 2500, 2600, 5000, 5100, 5200};
      for (int i = 0; i < clocks.length; i++) {
        host.setStateTtlProcessingTime(clocks[i]);
        nativeHarness.setStateTtlProcessingTime(clocks[i]);
        host.setProcessingTime(clocks[i]);
        nativeHarness.setProcessingTime(clocks[i]);
        var row =
            GenericRowData.of(
                i % 3 == 0 ? null : 1,
                (long) i + 1,
                TimestampData.fromEpochMillis(1000 + i / 2 * 400));
        host.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator, true))));
        row.setField(1, 999999L);
        drain(host, nativeHarness, expected, actual);
        assertEquals(expected, actual, "after input " + i);
        if (i == 2) {
          host.prepareSnapshotPreBarrier(1);
          nativeHarness.prepareSnapshotPreBarrier(1);
          drain(host, nativeHarness, expected, actual);
          var hostState = host.snapshot(1, clocks[i]);
          var nativeState = nativeHarness.snapshot(1, clocks[i]);
          if (restore) {
            host.close();
            nativeHarness.close();
            host =
                new KeyedOneInputStreamOperatorTestHarness<>(
                    new KeyedProcessOperator<>(function(frame)),
                    selector,
                    selector.getProducedType());
            host.setup(new RowDataSerializer(OUTPUT));
            nativeHarness = nativeHarness(frame);
            host.initializeState(hostState);
            nativeHarness.initializeState(nativeState);
            host.open();
            nativeHarness.open();
          }
        }
        if (i == 3 || i == 6) {
          long timestamp = i == 3 ? 1400 : 1800;
          host.processWatermark(new Watermark(timestamp));
          nativeHarness.processWatermark(new Watermark(timestamp));
          drain(host, nativeHarness, expected, actual);
          assertEquals(expected, actual, "after watermark " + timestamp);
        }
      }
      host.processWatermark(Watermark.MAX_WATERMARK);
      nativeHarness.processWatermark(Watermark.MAX_WATERMARK);
      drain(host, nativeHarness, expected, actual);
      assertEquals(expected, actual);
      assertFalse(expected.isEmpty());
    } finally {
      host.close();
      nativeHarness.close();
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch>
      nativeHarness(int frame) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            Thread.currentThread().getContextClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(INPUT));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch>(
            new NativeColumnarJvmOverOperator(INPUT, OUTPUT, function(frame), selector, 3),
            new ArrowBatchRowKeySelector(INPUT, selector),
            selector.getProducedType());
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static KeyedProcessFunction<RowData, RowData, RowData> function(int frame) {
    var generated =
        new GeneratedAggsHandleFunction("OverSum", "", new Object[0]) {
          @Override
          public AggsHandleFunction newInstance(ClassLoader loader) {
            return new NativeJvmAggregateTtlTest.SumHandler();
          }
        };
    LogicalType[] accumulators = {new BigIntType(), new BigIntType()};
    LogicalType[] input = INPUT.getChildren().toArray(new LogicalType[0]);
    switch (frame) {
      case 0:
        return new ProcTimeUnboundedPrecedingFunction(
            org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig(1000),
            generated,
            accumulators);
      case 1:
        return new ProcTimeRowsBoundedPrecedingFunction(
            1000, 1500, generated, accumulators, input, 3);
      case 2:
        return new RowTimeRowsBoundedPrecedingFunction(
            1000, 1500, generated, accumulators, input, 3, 2);
      case 3:
        return new RowTimeRangeBoundedPrecedingFunction(generated, accumulators, input, 1000, 2);
      case 4:
        return new RowTimeRowsUnboundedPrecedingFunction(
            1000, 1500, generated, accumulators, input, 2);
      case 5:
        return new RowTimeRangeUnboundedPrecedingFunction(
            1000, 1500, generated, accumulators, input, 2);
      default:
        throw new IllegalArgumentException();
    }
  }

  private static void drain(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host,
      KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch> nativeHarness,
      List<String> expected,
      List<String> actual) {
    for (Object value : host.getOutput())
      if (value instanceof StreamRecord)
        expected.add(value((RowData) ((StreamRecord<?>) value).getValue()));
      else if (value instanceof Watermark) expected.add(value.toString());
    host.getOutput().clear();
    for (Object value : nativeHarness.getOutput())
      if (value instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) value).getValue()).root()) {
          var reader = ArrowConversion.createArrowReader(root, OUTPUT);
          for (int i = 0; i < root.getRowCount(); i++) actual.add(value(reader.read(i)));
        }
      } else if (value instanceof Watermark) actual.add(value.toString());
    nativeHarness.getOutput().clear();
  }

  private static String value(RowData row) {
    return (row.isNullAt(0) ? null : row.getInt(0))
        + "|"
        + row.getLong(1)
        + "|"
        + row.getTimestamp(2, 3)
        + "|"
        + row.getLong(3);
  }
}
