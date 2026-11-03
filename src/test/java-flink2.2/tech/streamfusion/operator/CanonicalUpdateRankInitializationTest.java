package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.*;
import static tech.streamfusion.operator.CanonicalUpdateRankStateInteropTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;

class CanonicalUpdateRankInitializationTest {
  static Stream<Arguments> modes() {
    var modes = new java.util.ArrayList<Arguments>();
    for (boolean rocks : new boolean[] {false, true})
      for (boolean nativeFirst : new boolean[] {false, true})
        for (boolean before : new boolean[] {false, true})
          for (boolean projected : new boolean[] {false, true})
            modes.add(Arguments.of(rocks, nativeFirst, before, projected));
    return modes.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void actualOperatorStateSurvivesBothSwitchOrdersAndFurtherUpdates(
      boolean rocks, boolean nativeFirst, boolean before, boolean projected) throws Exception {
    var output = projected ? OUTPUT : INPUT;
    var prefix =
        List.<RowData>of(
            GenericRowData.of(1L, 10L, 1L), GenericRowData.of(1L, 20L, 2L),
            GenericRowData.of(1L, 20L, 3L), GenericRowData.of(1L, 30L, 4L),
            GenericRowData.of(2L, 7L, 1L), GenericRowData.of(2L, 7L, 2L));
    var suffix =
        List.<RowData>of(
            update(1L, 5L, 4L),
            update(1L, 15L, 3L),
            update(2L, 6L, 2L),
            GenericRowData.of(3L, 8L, 1L),
            GenericRowData.of(1L, 15L, 5L));
    var future =
        List.<RowData>of(
            update(1L, 4L, 1L),
            update(2L, 6L, 1L),
            update(3L, 3L, 1L),
            GenericRowData.of(2L, 4L, 3L),
            update(1L, 3L, 5L));
    try (var reference = harness(true, rocks, before, projected);
        var firstStock = harness(true, rocks, before, projected);
        var firstNative = nativeHarness(rocks, before, projected)) {
      reference.open();
      feed(reference, INPUT, prefix);
      OperatorSubtaskState checkpoint;
      if (nativeFirst) {
        firstNative.open();
        push(firstNative, prefix);
        assertEquals(
            bytes(output, reference.extractOutputValues()),
            bytes(output, collect(firstNative, output)));
        checkpoint = snapshot(firstNative);
      } else {
        firstStock.open();
        feed(firstStock, INPUT, prefix);
        checkpoint = snapshot(firstStock);
      }
      try (var secondStock = harness(true, rocks, before, projected);
          var secondNative = nativeHarness(rocks, before, projected)) {
        if (nativeFirst) {
          secondStock.initializeState(checkpoint);
          secondStock.open();
          feed(secondStock, INPUT, suffix);
          checkpoint = snapshot(secondStock);
        } else {
          secondNative.initializeState(checkpoint);
          secondNative.open();
          push(secondNative, suffix);
          checkpoint = snapshot(secondNative);
        }
        feed(reference, INPUT, suffix);
        var expected = bytes(output, reference.extractOutputValues());
        assertFalse(expected.isEmpty());
        assertEquals(
            expected,
            nativeFirst
                ? bytes(output, secondStock.extractOutputValues())
                : bytes(output, collect(secondNative, output)));
        try (var thirdStock = harness(true, rocks, before, projected);
            var thirdNative = nativeHarness(rocks, before, projected)) {
          if (nativeFirst) {
            thirdNative.initializeState(checkpoint);
            thirdNative.open();
            push(thirdNative, future);
          } else {
            thirdStock.initializeState(checkpoint);
            thirdStock.open();
            feed(thirdStock, INPUT, future);
          }
          feed(reference, INPUT, future);
          expected = bytes(output, reference.extractOutputValues());
          assertFalse(expected.isEmpty());
          assertEquals(
              expected,
              nativeFirst
                  ? bytes(output, collect(thirdNative, output))
                  : bytes(output, thirdStock.extractOutputValues()));
        }
      }
    }
  }

  private static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(2);
    return harness
        .snapshotWithLocalState(
            2,
            0,
            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static RowData update(long key, long sort, long id) {
    var row = GenericRowData.of(key, sort, id);
    row.setRowKind(RowKind.UPDATE_AFTER);
    return row;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      List<RowData> rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED, true))));
    harness.prepareSnapshotPreBarrier(0);
  }

  private KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      boolean rocks, boolean before, boolean projected) throws Exception {
    var contract = contract(false);
    var operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {Native.STOCK_KEY_PROJECTION, -1},
            INPUT,
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            3,
            projected,
            false,
            new int[] {0, 2},
            new int[] {Native.STOCK_KEY_PROJECTION, -1, -1},
            before,
            false,
            -1,
            0,
            128);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          "state.backend.type",
          tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        getClass().getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }
}
