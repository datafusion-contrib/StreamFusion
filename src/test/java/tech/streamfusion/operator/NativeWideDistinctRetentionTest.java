package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeWideDistinctRetentionTest {
  private static final RowType INPUT =
      RowType.of(
          new BigIntType(),
          new DecimalType(38, 0),
          new ArrayType(RowType.of(new DecimalType(38, 0), new BigIntType())));
  private static final RowType OUTPUT = RowType.of(new BigIntType(), new DecimalType(38, 0));

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retentionUsesFlushTimeAcrossCheckpoint(boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      input(harness, allocator, 0, 1);
      assertEquals(List.of(), collect(harness));
      harness.setProcessingTime(10);
      harness.processWatermark(new Watermark(1));
      assertEquals(List.of("+I:1"), collect(harness));
      input(harness, allocator, 90, 2);
      harness.setProcessingTime(120);
      harness.getOneInputOperator().prepareSnapshotPreBarrier(1L);
      snapshot = harness.snapshot(1L, 120L);
      assertEquals(List.of("+I:2"), collect(harness));
    }
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(rocks)) {
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      input(harness, allocator, 200, 3);
      harness.setProcessingTime(215);
      harness.processWatermark(new Watermark(2));
      assertEquals(List.of("-U:2", "+U:5"), collect(harness));
      input(harness, allocator, 300, 2);
      input(harness, allocator, 320, 3);
      harness.setProcessingTime(340);
      harness.processWatermark(new Watermark(3));
      assertEquals(List.of("+I:5"), collect(harness));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean rocks) throws Exception {
    var operator =
        new NativeColumnarGroupAggregateOperator(
            new int[] {9},
            new int[] {5800},
            new int[] {1},
            new int[] {0},
            new int[] {-1},
            new int[] {-1},
            new int[] {2},
            -1,
            true,
            true,
            1000,
            100,
            new int[] {-1},
            128);
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator,
            (ArrowBatch batch) -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
            Types.INT,
            128,
            1,
            0);
    if (rocks) {
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeWideDistinctRetentionTest.class.getClassLoader()));
    }
    return harness;
  }

  private static void input(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      BufferAllocator allocator,
      long time,
      long value)
      throws Exception {
    harness.setProcessingTime(time);
    DecimalData decimal = DecimalData.fromBigDecimal(BigDecimal.valueOf(value), 38, 0);
    RowData row =
        GenericRowData.of(
            1L, decimal, new GenericArrayData(new Object[] {GenericRowData.of(decimal, 1L)}));
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator, false))));
  }

  private static List<String> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<String> result = new ArrayList<>();
    for (Object output : harness.getOutput()) {
      Object batchCandidate;
      if (output instanceof StreamRecord<?>
          && (batchCandidate = ((StreamRecord<?>) output).getValue()) instanceof ArrowBatch) {
        ArrowBatch batch = ((ArrowBatch) batchCandidate);

        try (VectorSchemaRoot root = batch.root()) {
          for (RowData row : RowDataArrowConverter.read(root, OUTPUT)) {
            result.add(
                row.getRowKind().shortString()
                    + ":"
                    + row.getDecimal(1, 38, 0).toBigDecimal().toPlainString());
          }
        }
      }
    }
    harness.getOutput().clear();
    return result;
  }
}
