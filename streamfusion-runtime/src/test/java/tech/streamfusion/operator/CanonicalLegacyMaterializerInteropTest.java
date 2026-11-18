package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;
import tech.streamfusion.state.StockSinkMaterializerStateContract;

class CanonicalLegacyMaterializerInteropTest {
  static final RowType TYPE = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType KEY = RowType.of(new BigIntType());

  @BeforeAll
  static void memory() {
    CanonicalProcessingTimeWindowOperatorTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            key ->
                Stream.of(false, true)
                    .flatMap(
                        ttl -> Stream.of(false, true).map(rocks -> Arguments.of(key, ttl, rocks))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void frozenCandidateListsAndOriginalAgesContinueIntoReleasedStock(
      boolean inputKey, boolean ttl, boolean rocks) throws Exception {
    var expected = new OperatorSubtaskState[2];
    var captured = new OperatorSnapshotFutures[2];
    long retention = ttl ? 1000 : 0;
    try (var reference = stock(inputKey, retention, false);
        var source = nativeHarness(inputKey, retention, rocks)) {
      reference.open();
      CanonicalAppendRankTypedTest.time(reference, 100);
      for (var row :
          List.of(
              row(RowKind.INSERT, 1L, 10, 100),
              row(RowKind.INSERT, 1L, 20, 200),
              row(RowKind.UPDATE_AFTER, 1L, 10, 101),
              row(RowKind.INSERT, 2L, 30, 300),
              row(RowKind.INSERT, null, 70, 700)))
        reference.processElement(new StreamRecord<>(row));
      expected[0] = FirstNCanonicalTestSupport.snapshot(reference);
      source.initializeState(expected[0]);
      source.open();
      CanonicalAppendRankTypedTest.time(source, 100);
      var owner = (NativeSinkMaterializer) source.getOperator();
      assertEquals(rocks, owner.directRocksDBState());
      assertEquals(inputKey ? 4 : 5, headerRows(owner));
      try {
        captured[0] = CanonicalDistinctWindowAsyncTest.capture(source, 2);
        CanonicalAppendRankTypedTest.time(reference, 200);
        CanonicalAppendRankTypedTest.time(source, 200);
        var bridge = row(RowKind.INSERT, 1L, 40, 400);
        reference.processElement(new StreamRecord<>(new RowDataSerializer(TYPE).copy(bridge)));
        push(source, bridge);
        expected[1] = FirstNCanonicalTestSupport.snapshot(reference);
        captured[1] = CanonicalDistinctWindowAsyncTest.capture(source, 3);
        push(source, row(RowKind.INSERT, 1L, 50, 500));
        source.prepareSnapshotPreBarrier(4);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var stock = stock(inputKey, retention, false);
              var returned = stock(inputKey, retention, true)) {
            stock.initializeState(expected[stage]);
            returned.initializeState(saved);
            stock.open();
            returned.open();
            for (long clock : new long[] {1099, 1150, 2200}) {
              CanonicalAppendRankTypedTest.time(stock, clock);
              CanonicalAppendRankTypedTest.time(returned, clock);
              var rows =
                  clock == 1099
                      ? List.of(row(RowKind.DELETE, 1L, 99, 999))
                      : List.of(
                          row(RowKind.INSERT, 1L, 60, clock),
                          row(RowKind.DELETE, 1L, 20, 200),
                          row(RowKind.DELETE, 2L, 30, 300),
                          row(RowKind.INSERT, null, 80, clock));
              for (var value : rows) {
                stock.processElement(new StreamRecord<>(new RowDataSerializer(TYPE).copy(value)));
                returned.processElement(
                    new StreamRecord<>(new RowDataSerializer(TYPE).copy(value)));
              }
              assertEquals(
                  CanonicalAppendRankTypedTest.bytes(TYPE, stock.extractOutputValues()),
                  CanonicalAppendRankTypedTest.bytes(TYPE, returned.extractOutputValues()));
              stock.getOutput().clear();
              returned.getOutput().clear();
            }
          } finally {
            saved.discardState();
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
        CanonicalDistinctWindowAsyncTest.discardOutput(source);
      }
    } finally {
      for (var state : expected) if (state != null) state.discardState();
    }
  }

  @Test
  void hotCandidateListCrossesNativeTransferChunks() throws Exception {
    OperatorSubtaskState original;
    try (var stock = stock(false, 0, false)) {
      stock.open();
      for (int i = 0; i < 2051; i++)
        stock.processElement(new StreamRecord<>(row(RowKind.INSERT, 1L, i, i)));
      original = FirstNCanonicalTestSupport.snapshot(stock);
    }
    try (var source = nativeHarness(false, 0, true)) {
      source.initializeState(original);
      source.open();
      assertTrue(((NativeSinkMaterializer) source.getOperator()).directRocksDBState());
      assertEquals(2051, headerRows((NativeSinkMaterializer) source.getOperator()));
      var captured = CanonicalDistinctWindowAsyncTest.capture(source, 2);
      try {
        push(source, row(RowKind.INSERT, 1L, 9999, 9999));
        source.prepareSnapshotPreBarrier(3);
        var owner = (NativeSinkMaterializer) source.getOperator();
        owner.closeHandle();
        owner.handle = 0;
        var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured);
        try (var stock = stock(false, 0, true);
            var returned = stock(false, 0, false)) {
          stock.initializeState(original);
          returned.initializeState(saved);
          stock.open();
          returned.open();
          for (var value :
              List.of(row(RowKind.DELETE, 1L, 2050, 2050), row(RowKind.DELETE, 1L, 2049, 2049))) {
            stock.processElement(new StreamRecord<>(new RowDataSerializer(TYPE).copy(value)));
            returned.processElement(new StreamRecord<>(new RowDataSerializer(TYPE).copy(value)));
          }
          var rows = stock.extractOutputValues();
          assertFalse(rows.isEmpty());
          assertEquals(
              CanonicalAppendRankTypedTest.bytes(TYPE, rows),
              CanonicalAppendRankTypedTest.bytes(TYPE, returned.extractOutputValues()));
        } finally {
          saved.discardState();
        }
      } finally {
        captured.cancel();
        CanonicalDistinctWindowAsyncTest.discardOutput(source);
      }
    } finally {
      original.discardState();
    }
  }

  static long headerRows(NativeSinkMaterializer operator) {
    long[] count = {0};
    tech.streamfusion.Native.exportSinkMaterializerCanonical(
        operator.handle,
        0,
        (a, s) -> {
          try (var array = org.apache.arrow.c.ArrowArray.wrap(a);
              var schema = org.apache.arrow.c.ArrowSchema.wrap(s);
              var root =
                  org.apache.arrow.c.Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var values = (org.apache.arrow.vector.BigIntVector) root.getVector(5);
            for (int i = 0; i < root.getRowCount(); i++) count[0] += values.get(i);
          } catch (Exception failure) {
            throw new IllegalStateException(failure);
          }
        });
    return count[0];
  }

  static RowData row(RowKind kind, Long key, long value, long payload) {
    return new RowDataSerializer(TYPE)
        .toBinaryRow(GenericRowData.ofKind(kind, key, value, payload))
        .copy();
  }

  static GeneratedRecordEqualiser equaliser(int arity) {
    String name = "LegacyMaterializerEqualiser" + arity;
    String code =
        "public class "
            + name
            + " implements org.apache.flink.table.runtime.generated.RecordEqualiser {"
            + " public "
            + name
            + "(Object[] refs){} public boolean equals(org.apache.flink.table.data.RowData a,"
            + " org.apache.flink.table.data.RowData b){for(int i=0;i<"
            + arity
            + ";i++){if(a.isNullAt(i)!=b.isNullAt(i))return"
            + " false;if(!a.isNullAt(i)&&a.getLong(i)!=b.getLong(i))return false;}return"
            + " true;}public boolean equalsWithoutHeader(org.apache.flink.table.data.RowData"
            + " a,org.apache.flink.table.data.RowData b){return equals(a,b);}}";
    return new GeneratedRecordEqualiser(name, code, new Object[0]);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      boolean key, long ttl, boolean rocks) throws Exception {
    var operator =
        new SinkUpsertMaterializer(
            ttl == 0
                ? StateTtlConfig.DISABLED
                : org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig(ttl),
            new RowDataSerializer(TYPE),
            equaliser(3),
            equaliser(1),
            key ? new int[] {1} : null);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalLegacyMaterializerInteropTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(TYPE));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    if (rocks) harness.setStateBackend(tech.streamfusion.compat.CanonicalTestCompat.rocksBackend());
    harness.setup(new RowDataSerializer(TYPE));
    return harness;
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      boolean key, long ttl, boolean rocks) throws Exception {
    var operator =
        new NativeSinkMaterializer(
            new int[] {0}, new int[] {-1}, key ? new int[] {1} : null, ttl, 128);
    var contract =
        new StockSinkMaterializerStateContract(
            new RowDataSerializer(KEY),
            KEY,
            TYPE,
            key ? new int[] {1} : null,
            StockSinkMaterializerStateContract.Strategy.LEGACY,
            ttl);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        StreamExecutionEnvironment.getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalLegacyMaterializerInteropTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(
                config, CanonicalLegacyMaterializerInteropTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowData value)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(List.of(value), TYPE, NativeAllocator.SHARED, true))));
  }
}
