package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;

class CanonicalGroupArithmeticInitializationTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());

  static Stream<Arguments> cases() {
    return Stream.of("arithmetic", "hidden", "count", "star")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                first ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            before ->
                                                                Arguments.of(
                                                                    mode, rocks, ttl, first,
                                                                    before))))));
  }

  @ParameterizedTest(name = "{0}, rocks={1}, ttl={2}, native-first={3}, before={4}")
  @MethodSource("cases")
  void actualGeneratedOperatorsResumeThroughSuccessiveCanonicalSwitches(
      String mode, boolean rocks, long ttl, boolean nativeFirst, boolean before) throws Exception {
    var steps =
        List.of(
            row(1L, null, 0),
            row(2L, 11L, 0),
            row(null, 5L, 0),
            row(1L, 7L, 0),
            row(1L, 4L, 0),
            row(2L, 11L, 3),
            row(1L, 7L, 1),
            row(1L, 4L, 3),
            row(null, 5L, 3),
            row(1L, null, 3),
            row(1L, 17L, 0),
            row(2L, null, 0));
    try (var reference = new GroupAggregateStockTestFixture(mode, rocks, ttl, before)) {
      reference.harness.open();
      OperatorSubtaskState saved;
      try (var stock = new GroupAggregateStockTestFixture(mode, rocks, ttl, before);
          var nativeHarness = nativeHarness(stock, rocks)) {
        if (nativeFirst) nativeHarness.open();
        else stock.harness.open();
        for (int i = 0; i < 3; i++) {
          var expected = stockStep(reference, steps.get(i), now(i));
          assertEquals(
              expected,
              nativeFirst
                  ? nativeStep(nativeHarness, steps.get(i), now(i), reference.outputType)
                  : stockStep(stock, steps.get(i), now(i)),
              "prefix " + i);
        }
        saved = snapshot(nativeFirst ? nativeHarness : stock.harness);
        assertState(reference, saved, mode, rocks, ttl, before);
      }
      try (var stock = new GroupAggregateStockTestFixture(mode, rocks, ttl, before);
          var nativeHarness = nativeHarness(stock, rocks)) {
        var receiver = nativeFirst ? stock.harness : nativeHarness;
        receiver.initializeState(saved);
        receiver.open();
        // Re-export before any new input, preserving NULL accumulators and original TTL ages.
        saved = snapshot(receiver);
        assertState(reference, saved, mode, rocks, ttl, before);
      }
      try (var stock = new GroupAggregateStockTestFixture(mode, rocks, ttl, before);
          var nativeHarness = nativeHarness(stock, rocks)) {
        var receiver = nativeFirst ? stock.harness : nativeHarness;
        receiver.initializeState(saved);
        receiver.open();
        for (int i = 3; i < 5; i++) {
          var expected = stockStep(reference, steps.get(i), now(i));
          assertEquals(
              expected,
              nativeFirst
                  ? stockStep(stock, steps.get(i), now(i))
                  : nativeStep(nativeHarness, steps.get(i), now(i), reference.outputType),
              "middle " + i);
        }
        saved = snapshot(receiver);
        assertState(reference, saved, mode, rocks, ttl, before);
      }
      try (var stock = new GroupAggregateStockTestFixture(mode, rocks, ttl, before);
          var nativeHarness = nativeHarness(stock, rocks)) {
        var receiver = nativeFirst ? nativeHarness : stock.harness;
        receiver.initializeState(saved);
        receiver.open();
        for (int i = 5; i < steps.size(); i++) {
          var expected = stockStep(reference, steps.get(i), now(i));
          assertEquals(
              expected,
              nativeFirst
                  ? nativeStep(nativeHarness, steps.get(i), now(i), reference.outputType)
                  : stockStep(stock, steps.get(i), now(i)),
              "suffix " + i);
          if (i == 7) assertState(reference, snapshot(receiver), mode, rocks, ttl, before);
        }
        assertState(reference, snapshot(receiver), mode, rocks, ttl, before);
      }
    }
  }

  static Stream<Arguments> overflowCases() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(false, true)
                    .flatMap(
                        before ->
                            Stream.of(Long.MAX_VALUE, Long.MIN_VALUE)
                                .map(count -> Arguments.of(rocks, before, count))));
  }

  @ParameterizedTest
  @MethodSource("overflowCases")
  @SuppressWarnings("unchecked")
  void restoredLongCountersFollowReleasedWrappingArithmetic(
      boolean rocks, boolean before, long initial) throws Exception {
    try (var reference = new GroupAggregateStockTestFixture("star", rocks, 0, before);
        var source = new GroupAggregateStockTestFixture("star", rocks, 0, before)) {
      reference.harness.open();
      source.harness.open();
      for (var fixture : List.of(reference, source)) {
        var backend = fixture.harness.getOperator().<RowData>getKeyedStateBackend();
        backend.setCurrentKey(fixture.selector.getKey(row(1L, 7L, 0)));
        ((org.apache.flink.api.common.state.ValueState<RowData>) fixture.rawState())
            .update(GenericRowData.of(initial));
        assertEquals(1, fixture.rawStateBytes().size(), "nonempty stock overflow source");
      }
      OperatorSubtaskState saved = snapshot(source.harness);
      int kind = initial == Long.MAX_VALUE ? 0 : 3;
      try (var nativeHarness = nativeHarness(source, rocks)) {
        nativeHarness.initializeState(saved);
        nativeHarness.open();
        assertEquals(
            stockStep(reference, row(1L, 7L, kind), 1000),
            nativeStep(nativeHarness, row(1L, 7L, kind), 1000, reference.outputType));
        saved = snapshot(nativeHarness);
        assertState(reference, saved, "star", rocks, 0, before);
      }
      try (var returned = new GroupAggregateStockTestFixture("star", rocks, 0, before)) {
        returned.harness.initializeState(saved);
        returned.harness.open();
        int inverse = kind == 0 ? 3 : 0;
        assertEquals(
            stockStep(reference, row(1L, 7L, inverse), 1100),
            stockStep(returned, row(1L, 7L, inverse), 1100));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }

  static void assertState(
      GroupAggregateStockTestFixture reference,
      OperatorSubtaskState saved,
      String mode,
      boolean rocks,
      long ttl,
      boolean before)
      throws Exception {
    var expected = reference.rawStateBytes();
    try (var restored = new GroupAggregateStockTestFixture(mode, rocks, ttl, before)) {
      restored.harness.initializeState(saved);
      restored.harness.open();
      assertEquals(
          expected, restored.rawStateBytes(), "complete raw accumulator bytes and write ages");
    }
  }

  private static long now(int index) {
    return index < 8 ? 1000 + 100L * index : 3000 + 100L * index;
  }

  static GenericRowData row(Long key, Long value, int kind) {
    var row = GenericRowData.of(key, value);
    row.setRowKind(RowKind.fromByteValue((byte) kind));
    return row;
  }

  private static List<String> bytes(List<RowData> rows, RowType type) throws Exception {
    var serializer = new RowDataSerializer(type);
    var result = new ArrayList<String>();
    for (var row : rows) {
      var out = new DataOutputSerializer(64);
      serializer.serialize(row, out);
      result.add(Base64.getEncoder().encodeToString(out.getCopyOfBuffer()));
    }
    return result;
  }

  static List<String> stockStep(GroupAggregateStockTestFixture fixture, RowData row, long time)
      throws Exception {
    fixture.harness.setProcessingTime(time);
    fixture.harness.setStateTtlProcessingTime(time);
    fixture.harness.getOutput().clear();
    fixture.harness.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(row)));
    return bytes(fixture.harness.extractOutputValues(), fixture.outputType);
  }

  static List<String> nativeStep(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowData row,
      long time,
      RowType output)
      throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(List.of(row), INPUT, NativeAllocator.SHARED, true))));
    harness.prepareSnapshotPreBarrier(0);
    var rows = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(output);
    Object element;
    while ((element = harness.getOutput().poll()) != null) {
      if (!(element instanceof StreamRecord)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) element).getValue()).root()) {
        for (var value : RowDataArrowConverter.read(root, output)) rows.add(serializer.copy(value));
      }
    }
    return bytes(rows, output);
  }

  static OperatorSubtaskState snapshot(OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
    if (harness.getOperator() instanceof DirectGroup)
      ((DirectGroup) harness.getOperator()).assertStorage();
    harness.prepareSnapshotPreBarrier(1);
    return harness
        .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      GroupAggregateStockTestFixture fixture, boolean rocks) throws Exception {
    var operator = new DirectGroup(fixture, rocks);
    var contract = fixture.contract();
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalGroupArithmeticInitializationTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(
                config, CanonicalGroupArithmeticInitializationTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  static final class DirectGroup extends NativeColumnarGroupAggregateOperator {
    private final boolean persistent;
    boolean checkpoint;

    DirectGroup(GroupAggregateStockTestFixture fixture, boolean persistent) {
      super(
          fixture.kinds,
          new int[fixture.kinds.length],
          fixture.valueColumns,
          new int[] {0},
          new int[0],
          new int[0],
          new int[0],
          -1,
          fixture.before,
          false,
          -1,
          fixture.ttl,
          new int[] {Native.STOCK_KEY_PROJECTION, -1},
          128);
      this.persistent = persistent;
    }

    long address() {
      return handle;
    }

    boolean persistent() {
      return directRocksDBState();
    }

    void assertStorage() {
      assertEquals(persistent, directRocksDBState(), "actual native state backend");
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      if (checkpoint) return super.snapshotRawPartitions();
      throw new AssertionError("canonical export used a native snapshot");
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (checkpoint) return super.snapshotCanonicalPartitions();
      throw new AssertionError("canonical export used native canonical partitions");
    }

    @Override
    protected String[] checkpointRocksDBHandle(String path) {
      if (checkpoint) return super.checkpointRocksDBHandle(path);
      throw new AssertionError("canonical export checkpointed native RocksDB");
    }
  }
}
