package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.StringCacheColumns;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

/**
 * Internal constant columns and native checkpoint ownership; stock schemas are tested separately.
 */
class NativeOverConstantArgumentsTest {
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new DoubleType(),
            new TimestampType(3),
            new VarCharType(VarCharType.MAX_LENGTH)
          },
          new String[] {"__over_constant_0", "v", "rt", "payload"});
  private static final RowType CONSTANTS =
      RowType.of(new BigIntType(false), new DoubleType(), new BigIntType(), new DecimalType(20, 2));
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new DoubleType(),
          new TimestampType(3),
          new VarCharType(VarCharType.MAX_LENGTH),
          new BigIntType(),
          new DoubleType(),
          new BigIntType(),
          new DoubleType(),
          new DecimalType(38, 6));

  static Stream<Arguments> modes() {
    var cases = new ArrayList<Arguments>();
    for (boolean proctime : new boolean[] {false, true})
      for (boolean rows : new boolean[] {false, true})
        for (boolean rocks : new boolean[] {false, true})
          for (boolean ttl : new boolean[] {false, true})
            for (boolean rowKind : new boolean[] {false, true})
              for (boolean canonical : new boolean[] {false, true})
                cases.add(Arguments.of(proctime, rows, rocks, ttl, rowKind, canonical));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void constantsSurviveNativeCheckpointsAndDoNotCopyOrRetainTheInputView(
      boolean proctime,
      boolean rows,
      boolean rocks,
      boolean ttl,
      boolean rowKind,
      boolean canonical)
      throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    var actual = new ArrayList<String>();
    OperatorSubtaskState snapshot;
    try (var source = harness(proctime, rows, rocks, ttl, encoded())) {
      source.open();
      source.processWatermark(new Watermark(0));
      source.setProcessingTime(100);
      var input =
          batch(
              rowKind,
              row(1L, 10.0, 100, "α"),
              row(1L, null, 200, "😀"),
              row(null, null, 300, "z"));
      long address = input.root().getVector(3).getDataBuffer().memoryAddress();
      source.processElement(new StreamRecord<>(input));
      if (proctime) {
        var output = source.extractOutputValues().get(0).root();
        assertEquals(address, output.getVector(3).getDataBuffer().memoryAddress());
        assertNotNull(output.getVector(StringCacheColumns.name(3)));
        actual.addAll(drain(source));
      } else assertTrue(source.extractOutputValues().isEmpty());
      snapshot =
          canonical
              ? source
                  .snapshotWithLocalState(
                      1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState()
              : source.snapshot(1, 100);
    }
    try (var restored = harness(proctime, rows, rocks, ttl, encoded())) {
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(500);
      restored.processElement(
          new StreamRecord<>(
              batch(rowKind, row(1L, 3.0, 400, "tail"), row(null, 5.0, 500, "end"))));
      if (!proctime) restored.processWatermark(new Watermark(500));
      actual.addAll(drain(restored));
    }
    assertEquals(
        List.of(
            "1:α:1:-7.0:0:10.0:-0.070000",
            "1:😀:2:-14.0:0:10.0:-0.070000",
            "null:z:1:-7.0:0:null:-0.070000",
            "1:tail:3:-21.0:0:3.0:-0.070000",
            "null:end:2:-14.0:0:5.0:-0.070000"),
        actual);
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void malformedEncodedArgumentsReleaseTheInputAndAllCompanions(boolean rocks) throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var harness = harness(true, true, rocks, false, new byte[] {0})) {
      harness.open();
      assertThrows(
          RuntimeException.class,
          () ->
              harness.processElement(
                  new StreamRecord<>(batch(true, row(1L, 1.0, 100, "failure")))));
      assertTrue(harness.extractOutputValues().isEmpty());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static byte[] encoded() throws Exception {
    var bytes = new DataOutputSerializer(32);
    new RowDataSerializer(CONSTANTS)
        .serialize(
            GenericRowData.of(
                1L, -7.0, null, DecimalData.fromBigDecimal(new BigDecimal("-0.07"), 20, 2)),
            bytes);
    return bytes.getCopyOfBuffer();
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean proctime, boolean rows, boolean rocks, boolean ttl, byte[] constants)
      throws Exception {
    var operator =
        new NativeOverAggregateOperator(
                2,
                new int[] {4, 5, 6, 1, 7},
                new int[] {0},
                new int[] {0, 1, 0, 1, 4002},
                new int[] {3, 0, 3, 1, 4},
                rows ? 3 : 0,
                0,
                proctime,
                new int[] {Native.STOCK_KEY_PROJECTION, -1},
                ttl ? 1000 : 0,
                INPUT,
                128)
            .withConstantArguments(CONSTANTS, constants);
    // Constants are operator configuration and must survive task serialization without RowData.
    operator = org.apache.flink.util.InstantiationUtil.clone(operator);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeOverConstantArgumentsTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static RowData row(Long key, Double value, long time, String payload) {
    return GenericRowData.of(
        key, value, TimestampData.fromEpochMillis(time), StringData.fromString(payload));
  }

  private static ArrowBatch batch(boolean rowKind, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), INPUT, NativeAllocator.SHARED, rowKind, true));
  }

  private static List<String> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    var values = new ArrayList<String>();
    for (var output : harness.extractOutputValues()) {
      try (var root = output.root()) {
        assertEquals(
            OUTPUT.getFieldCount(),
            root.getFieldVectors().stream()
                .filter(
                    vector ->
                        !vector.getName().equals(RowDataArrowConverter.ROW_KIND_COLUMN)
                            && !vector.getName().equals(StringCacheColumns.name(3)))
                .count());
        for (var row : RowDataArrowConverter.read(root, OUTPUT)) {
          values.add(
              (row.isNullAt(0) ? "null" : row.getLong(0))
                  + ":"
                  + row.getString(3)
                  + ":"
                  + row.getLong(4)
                  + ":"
                  + row.getDouble(5)
                  + ":"
                  + row.getLong(6)
                  + ":"
                  + (row.isNullAt(7) ? "null" : row.getDouble(7))
                  + ":"
                  + row.getDecimal(8, 38, 6));
        }
      }
    }
    harness.getOutput().clear();
    return values;
  }
}
