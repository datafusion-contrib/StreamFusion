package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.StockFirstNStateContract;

class CanonicalFirstNRowKeyStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @org.junit.jupiter.api.AfterEach
  void releaseSnapshots() throws Exception {
    for (var state : snapshots) state.discardState();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (boolean nested : new boolean[] {false, true})
      for (RowKind kind : RowKind.values())
        for (boolean padding : new boolean[] {false, true})
          for (boolean rocks : new boolean[] {false, true})
            for (boolean rank : new boolean[] {false, true})
              for (long ttl : new long[] {0, 1000})
                cases.add(Arguments.of(nested, kind, padding, rocks, rank, ttl));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void binaryKeysAndLiveGenericFormsAddressTheSameRestoredCounters(
      boolean nested, RowKind kind, boolean padding, boolean rocks, boolean rank, long ttl)
      throws Exception {
    var input = RowType.of(CanonicalRowKeyFixture.type(nested), new BigIntType());
    var fields = new ArrayList<>(input.getFields());
    if (rank) fields.add(new RowType.RowField("rank", new BigIntType(false)));
    var output = new RowType(fields);
    try (var stock = FirstNCanonicalTestSupport.harness(true, false, rank, ttl, input, !rocks);
        var nativeOwner = nativeHarness(input, rocks, rank, ttl)) {
      stock.open();
      List<RowData> prefix =
          List.of(
              row(nested, kind, padding, false, 1L, 10),
              row(nested, kind, padding, true, 2L, 20),
              row(nested, kind, padding, false, null, 30));
      feed(stock, input, output, prefix, 1000);
      nativeOwner.initializeState(snapshot(stock));
      nativeOwner.open();
      assertEquals(
          rocks, ((NativeColumnarFirstNOperator) nativeOwner.getOperator()).directRocksDBState());
      try (var returned =
          FirstNCanonicalTestSupport.harness(true, false, rank, ttl, input, rocks)) {
        returned.initializeState(snapshot(nativeOwner));
        returned.open();
        assertEquals(
            CanonicalUnboundedOverScalarTest.raw(stock),
            CanonicalUnboundedOverScalarTest.raw(returned));
      }
      var suffix =
          List.of(
              row(nested, kind, padding, true, 1L, 11),
              row(nested, kind, padding, false, 1L, 12),
              row(nested, kind, padding, false, 2L, 21),
              row(nested, kind, padding, true, null, 31));
      assertEquals(
          feed(stock, input, output, suffix, 1500), push(nativeOwner, input, output, suffix, 1500));
      nativeOwner.prepareSnapshotPreBarrier(3);
      var checkpoint = nativeOwner.snapshot(3, 1600);
      snapshots.add(checkpoint);
      try (var recovered = nativeHarness(input, rocks, rank, ttl);
          var returned = FirstNCanonicalTestSupport.harness(true, false, rank, ttl, input, rocks)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        returned.initializeState(snapshot(recovered));
        returned.open();
        assertEquals(
            CanonicalUnboundedOverScalarTest.raw(stock),
            CanonicalUnboundedOverScalarTest.raw(returned));
        var future =
            List.of(
                row(nested, kind, padding, false, 1L, 13),
                row(nested, kind, padding, true, 1L, 14),
                row(nested, kind, padding, false, 2L, 22),
                row(nested, kind, padding, true, null, 32));
        var expected = feed(stock, input, output, future, 1800);
        assertEquals(expected, feed(returned, input, output, future, 1800));
        assertEquals(expected, push(recovered, input, output, future, 1800));
        var expired =
            List.of(
                row(nested, kind, padding, false, 1L, 15),
                row(nested, kind, padding, false, 2L, 23),
                row(nested, kind, padding, false, null, 33));
        expected = feed(stock, input, output, expired, 2800);
        assertEquals(expected, feed(returned, input, output, expired, 2800));
        assertEquals(expected, push(recovered, input, output, expired, 2800));
      }
    }
  }

  private OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(1);
    var saved =
        harness
            .snapshotWithLocalState(
                1,
                0,
                org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState();
    snapshots.add(saved);
    return saved;
  }

  private static RowData row(
      boolean nested, RowKind kind, boolean padding, boolean generic, Long id, long value) {
    return GenericRowData.of(
        generic
            ? CanonicalRowKeyFixture.logicalKey(nested, id, kind)
            : CanonicalRowKeyFixture.key(nested, id, kind, padding),
        value);
  }

  private static List<String> feed(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      RowType input,
      RowType output,
      List<RowData> rows,
      long time)
      throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
    harness.getOutput().clear();
    var serializer = new RowDataSerializer(input);
    for (var row : rows) harness.processElement(new StreamRecord<>(serializer.copy(row)));
    return CanonicalRowKeyFixture.bytes(output, harness.extractOutputValues());
  }

  private static List<String> push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType input,
      RowType output,
      List<RowData> rows,
      long time)
      throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
    harness.processElement(
        new StreamRecord<>(new ArrowBatch(CanonicalRowKeyFixture.arrow(rows, input))));
    harness.prepareSnapshotPreBarrier(0);
    var values = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(output);
    for (var batch : harness.extractOutputValues())
      try (var root = batch.root()) {
        for (var row : RowDataArrowConverter.read(root, output)) values.add(serializer.copy(row));
      }
    harness.getOutput().clear();
    return CanonicalRowKeyFixture.bytes(output, values);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(RowType input, boolean rocks, boolean rank, long ttl) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalFirstNRowKeyStateTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(input));
    var contract =
        new StockFirstNStateContract(
            selector.getProducedType().toSerializer(),
            selector.getProducedType().toRowType(),
            ttl > 0);
    var operator =
        new NativeColumnarFirstNOperator(
            new int[] {0},
            FlinkKeyGroupUtils.stockKeyDescriptor(input, new int[] {0}),
            3,
            rank,
            ttl,
            128);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalFirstNRowKeyStateTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalFirstNRowKeyStateTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }
}
