package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.*;

class CanonicalMaterializerRowKeyStateTest {
  @BeforeAll
  static void memory() {
    CanonicalProcessingTimeWindowOperatorTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of(false, true)
            .flatMap(
                nested ->
                    Stream.of(false, true)
                        .flatMap(
                            inputKey ->
                                Stream.of(0L, 1000L)
                                    .flatMap(
                                        ttl ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    rocks ->
                                                        Stream.of(false, true)
                                                            .map(
                                                                first ->
                                                                    Arguments.of(
                                                                        nested, inputKey, ttl,
                                                                        rocks, first)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void binaryRowKeysKeepCandidateMatchingAndOriginalAgesAcrossBothEngineOrders(
      boolean nested, boolean inputKey, long ttl, boolean rocks, boolean first) throws Exception {
    var type = RowType.of(CanonicalRowKeyFixture.type(nested), new BigIntType(), new BigIntType());
    var keyType = RowType.of(type.getTypeAt(0));
    int[] upsert = inputKey ? new int[] {0, 1} : null;
    OperatorSubtaskState saved = null;
    try (var reference = stock(type, upsert, ttl, !rocks)) {
      reference.open();
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = (stage % 2 == 0) == first;
        try (var stock = stock(type, upsert, ttl, !rocks);
            var nativeOwner = nativeHarness(type, keyType, upsert, ttl, rocks)) {
          var receiver = nativeStage ? nativeOwner : stock;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (nativeStage)
            assertEquals(
                rocks, ((NativeSinkMaterializer) nativeOwner.getOperator()).directRocksDBState());
          long time = stage == 0 ? 100 : stage == 1 ? 500 : 1500;
          CanonicalAppendRankTypedTest.time(reference, time);
          CanonicalAppendRankTypedTest.time(receiver, time);
          for (var row : rows(nested, stage)) {
            reference.getOutput().clear();
            reference.processElement(new StreamRecord<>(new RowDataSerializer(type).copy(row)));
            var expected = CanonicalRowKeyFixture.bytes(type, reference.extractOutputValues());
            List<String> actual;
            if (nativeStage) {
              nativeOwner.getOutput().clear();
              nativeOwner.processElement(
                  new StreamRecord<>(
                      new ArrowBatch(CanonicalRowKeyFixture.arrow(List.of(row), type))));
              var output = new ArrayList<RowData>();
              var serializer = new RowDataSerializer(type);
              for (var batch : nativeOwner.extractOutputValues())
                try (var root = batch.root()) {
                  for (var value : RowDataArrowConverter.read(root, type))
                    output.add(serializer.copy(value));
                }
              actual = CanonicalRowKeyFixture.bytes(type, output);
            } else {
              stock.getOutput().clear();
              stock.processElement(new StreamRecord<>(new RowDataSerializer(type).copy(row)));
              actual = CanonicalRowKeyFixture.bytes(type, stock.extractOutputValues());
            }
            assertEquals(expected, actual, "stage=" + stage + ",kind=" + row.getRowKind());
          }
          var previous = saved;
          if (nativeStage && stage == 0) {
            nativeOwner.prepareSnapshotPreBarrier(7);
            var checkpoint = nativeOwner.snapshot(7, time);
            try (var recovered = nativeHarness(type, keyType, upsert, ttl, rocks)) {
              recovered.initializeState(checkpoint);
              recovered.open();
              saved = FirstNCanonicalTestSupport.snapshot(recovered);
            } finally {
              checkpoint.discardState();
            }
          } else saved = FirstNCanonicalTestSupport.snapshot(receiver);
          if (previous != null) previous.discardState();
        }
      }
    } finally {
      if (saved != null) saved.discardState();
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      RowType type, int[] upsert, long ttl, boolean rocks) throws Exception {
    var loader = CanonicalMaterializerRowKeyStateTest.class.getClassLoader();
    var match = upsert == null ? type : RowType.of(type.getTypeAt(0), type.getTypeAt(1));
    var operator =
        new SinkUpsertMaterializer(
            StateConfigUtil.createTtlConfig(ttl),
            new RowDataSerializer(type),
            new EqualiserCodeGenerator(type, loader)
                .generateRecordEqualiser("RowKeyMaterializerEq"),
            new EqualiserCodeGenerator(match, loader)
                .generateRecordEqualiser("RowKeyMaterializerMatchEq"),
            upsert);
    var selector =
        KeySelectorUtil.getRowDataSelector(loader, new int[] {0}, InternalTypeInfo.of(type));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
    harness.setup(new RowDataSerializer(type));
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(RowType type, RowType keyType, int[] upsert, long ttl, boolean rocks)
          throws Exception {
    var operator =
        new NativeSinkMaterializer(
            new int[] {0},
            FlinkKeyGroupUtils.stockKeyDescriptor(type, new int[] {0}),
            upsert,
            ttl,
            128);
    var contract =
        new StockSinkMaterializerStateContract(
            new RowDataSerializer(keyType),
            keyType,
            type,
            upsert,
            StockSinkMaterializerStateContract.Strategy.LEGACY,
            ttl);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString("state.backend.type", RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalMaterializerRowKeyStateTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalMaterializerRowKeyStateTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static List<RowData> rows(boolean nested, int stage) {
    switch (stage) {
      case 0:
        return List.of(
            row(nested, RowKind.INSERT, 1L, 10, 100),
            row(nested, RowKind.INSERT, 1L, 20, 200),
            row(nested, RowKind.INSERT, 2L, 30, 300),
            row(nested, RowKind.INSERT, null, 70, 700));
      case 1:
        return List.of(
            row(nested, RowKind.UPDATE_AFTER, 1L, 10, 101),
            row(nested, RowKind.DELETE, 1L, 20, 200),
            row(nested, RowKind.INSERT, 2L, 40, 400));
      case 2:
        return List.of(
            row(nested, RowKind.UPDATE_BEFORE, 1L, 10, 101),
            row(nested, RowKind.UPDATE_AFTER, 1L, 10, 102),
            row(nested, RowKind.INSERT, null, 80, 800),
            row(nested, RowKind.DELETE, 2L, 40, 400));
      default:
        throw new AssertionError(stage);
    }
  }

  private static RowData row(boolean nested, RowKind kind, Long key, long id, long value) {
    return GenericRowData.ofKind(
        kind,
        CanonicalRowKeyFixture.key(
            nested, key, key == null ? RowKind.INSERT : RowKind.values()[key.intValue() % 4], true),
        id,
        value);
  }
}
