package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.CanonicalTestCompat;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.*;

class CanonicalDedupRowKeyStateTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of("proc-first", "proc-last", "row-first", "row-last", "buffered")
            .filter(mode -> !mode.equals("buffered") || CanonicalTestCompat.flink22())
            .flatMap(
                mode ->
                    Stream.of(false, true)
                        .flatMap(
                            nested ->
                                Stream.of(RowKind.values())
                                    .flatMap(
                                        kind ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    padding ->
                                                        Stream.of(false, true)
                                                            .flatMap(
                                                                rocks ->
                                                                    Stream.of(false, true)
                                                                        .map(
                                                                            ttl ->
                                                                                Arguments.of(
                                                                                    mode, nested,
                                                                                    kind, padding,
                                                                                    rocks,
                                                                                    ttl))))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void completeBinaryKeysAndNestedPayloadsKeepTheirStateAndContinuation(
      String mode, boolean nested, RowKind kind, boolean padding, boolean rocks, boolean ttl)
      throws Exception {
    var layout = new Layout(mode, nested, kind, padding, ttl ? 1000 : 0);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState fromStock = null, fromNative = null;
    try (var stock = layout.stock(rocks);
        var source = layout.nativeHarness(rocks)) {
      stock.open();
      source.open();
      time(stock, 100);
      time(source, 100);
      var duplicate = (GenericRowData) layout.row(1L, 10, 100);
      duplicate.setField(3, CanonicalRowKeyFixture.key(nested, 0L, kind, !padding));
      var prefix =
          List.of(
              layout.row(1L, 10, 100),
              duplicate,
              layout.row(2L, 20, 300),
              layout.row(1L, 11, 90),
              layout.row(null, 30, 400));
      layout.feed(stock, prefix);
      layout.push(source, prefix);
      stock.processWatermark(new Watermark(100));
      source.processWatermark(new Watermark(100));
      assertEquals(layout.drain(stock), layout.nativeOutput(source));
      fromStock = FirstNCanonicalTestSupport.snapshot(stock);
      fromNative = FirstNCanonicalTestSupport.snapshot(source);
      try (var returned = layout.stock(!rocks);
          var imported = layout.nativeHarness(!rocks)) {
        returned.initializeState(fromNative);
        imported.initializeState(fromStock);
        returned.open();
        imported.open();
        assertEquals(
            CanonicalUnboundedOverScalarTest.raw(stock),
            CanonicalUnboundedOverScalarTest.raw(returned),
            "Exact nested keys, raw values and TTL ages");
        for (long clock : new long[] {500, 1200}) {
          time(stock, clock);
          time(returned, clock);
          time(imported, clock);
          var rows =
              List.of(
                  layout.row(1L, 11, 90),
                  layout.row(2L, 25, 250),
                  layout.row(3L, 35, 350),
                  layout.row(null, 31, 450));
          layout.feed(stock, rows);
          layout.feed(returned, rows);
          layout.push(imported, rows);
          stock.processWatermark(new Watermark(clock == 500 ? 350 : 1000));
          returned.processWatermark(new Watermark(clock == 500 ? 350 : 1000));
          imported.processWatermark(new Watermark(clock == 500 ? 350 : 1000));
          var expected = layout.drain(stock);
          assertEquals(expected, layout.drain(returned));
          assertEquals(expected, layout.nativeOutput(imported));
        }
      }
    } finally {
      if (fromStock != null) fromStock.discardState();
      if (fromNative != null) fromNative.discardState();
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static void time(KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness, long clock)
      throws Exception {
    harness.setProcessingTime(clock);
    harness.setStateTtlProcessingTime(clock);
  }

  private static final class Layout {
    final String mode;
    final boolean nested, padding, buffered, rowtime, first;
    final RowKind kind;
    final long ttl;
    final RowType input, keys;
    final StockStateContract contract;

    Layout(String mode, boolean nested, RowKind kind, boolean padding, long ttl) {
      this.mode = mode;
      this.nested = nested;
      this.kind = kind;
      this.padding = padding;
      this.ttl = ttl;
      buffered = mode.equals("buffered");
      rowtime = buffered || mode.startsWith("row");
      first = buffered || mode.endsWith("first");
      var value = CanonicalRowKeyFixture.type(nested);
      input = RowType.of(value, new BigIntType(), new TimestampType(3), value);
      keys = RowType.of(value);
      var selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(input));
      contract =
          buffered
              ? new StockBufferedDedupStateContract(
                  new RowDataSerializer(keys), selector, keys, input, new int[] {0}, 2, ttl)
              : new StockDedupStateContract(
                  new RowDataSerializer(keys),
                  keys,
                  input,
                  first && !rowtime,
                  rowtime ? 2 : -1,
                  ttl);
    }

    RowData row(Long key, long value, long timestamp) {
      return GenericRowData.of(
          CanonicalRowKeyFixture.key(nested, key, kind, padding),
          value,
          TimestampData.fromEpochMillis(timestamp),
          CanonicalRowKeyFixture.key(nested, value % 5, kind, padding));
    }

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var harness =
          buffered
              ? new KeyedOneInputStreamOperatorTestHarness<>(
                  new KeyedProcessOperator<>(
                      CanonicalTestCompat.bufferedDedup(InternalTypeInfo.of(input), ttl, 2)),
                  KeySelectorUtil.getRowDataSelector(
                      getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(input)),
                  InternalTypeInfo.<RowData>of(keys),
                  128,
                  1,
                  0)
              : DedupCanonicalTestSupport.harness(
                  input, new int[] {0}, mode, ttl, true, true, true);
      var config = new Configuration();
      config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
      harness.setStateBackend(
          StateBackendLoader.loadStateBackendFromConfig(config, getClass().getClassLoader(), null));
      if (buffered) harness.setup(new RowDataSerializer(input));
      return harness;
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks) throws Exception {
      int[] descriptor = FlinkKeyGroupUtils.stockKeyDescriptor(input, new int[] {0});
      AbstractNativeStatefulOperator<ArrowBatch> owner =
          buffered
              ? new NativeColumnarDeduplicateOperator(new int[] {0}, descriptor, 2, input, ttl, 128)
                  .withBinaryRowKeys()
              : new NativeColumnarKeepLastDeduplicateOperator(
                  new int[] {0},
                  descriptor,
                  2,
                  input,
                  true,
                  true,
                  rowtime,
                  first,
                  false,
                  false,
                  3,
                  ttl,
                  128);
      owner.setStockStateContract(contract);
      var harness =
          new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
              (OneInputStreamOperator<ArrowBatch, ArrowBatch>) owner,
              ignored -> 0,
              org.apache.flink.api.common.typeinfo.Types.INT,
              128,
              1,
              0);
      var config = new Configuration();
      if (rocks)
        config.setString(
            StateBackendOptions.STATE_BACKEND.key(),
            RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          harness.getStreamConfig().getOperatorID().toHexString(),
          owner.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      harness.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      harness.setup(new ArrowBatchSerializer());
      return harness;
    }

    void feed(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
        List<RowData> rows)
        throws Exception {
      for (var row : rows)
        harness.processElement(
            new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
    }

    void push(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
        List<RowData> rows)
        throws Exception {
      var binary = new RowDataSerializer(input);
      var incoming = new ArrayList<RowData>();
      for (var row : rows) incoming.add(binary.toBinaryRow(row).copy());
      harness.processElement(
          new StreamRecord<>(new ArrowBatch(CanonicalRowKeyFixture.arrow(incoming, input))));
    }

    List<String> drain(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
      var result = CanonicalRowKeyFixture.bytes(input, harness.extractOutputValues());
      harness.getOutput().clear();
      return result;
    }

    List<String> nativeOutput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
      var rows = new ArrayList<RowData>();
      var serializer = new RowDataSerializer(input);
      for (var batch : harness.extractOutputValues())
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, input);
          var changes = root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          for (int i = 0; i < root.getRowCount(); i++) {
            var row = serializer.copy(reader.read(i));
            if (changes != null)
              row.setRowKind(
                  RowKind.fromByteValue(((org.apache.arrow.vector.TinyIntVector) changes).get(i)));
            rows.add(row);
          }
        }
      harness.getOutput().clear();
      return CanonicalRowKeyFixture.bytes(input, rows);
    }
  }
}
