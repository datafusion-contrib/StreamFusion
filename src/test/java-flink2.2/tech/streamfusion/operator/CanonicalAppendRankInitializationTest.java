package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankMatrixTest.INPUT;
import static tech.streamfusion.operator.CanonicalAppendRankMatrixTest.harness;
import static tech.streamfusion.operator.CanonicalAppendRankMatrixTest.selector;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.StockAppendTopNStateContract;

class CanonicalAppendRankInitializationTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void variableModeIsInstalledBeforeImportingPositiveAndBoundOnlyPartitions(boolean rocks)
      throws Exception {
    var keyType = RowType.of(new BigIntType());
    var output = RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
    var contract =
        new StockAppendTopNStateContract(
            new RowDataSerializer(keyType),
            selector(new int[] {0}),
            selector(new int[] {1}),
            keyType,
            INPUT,
            new int[] {1},
            false,
            true,
            false);
    var operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {Native.STOCK_KEY_PROJECTION, -1},
            INPUT,
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            100,
            true,
            false,
            null,
            null,
            true,
            false,
            -1,
            0,
            128,
            2,
            true);
    operator.setStockStateContract(contract);
    try (var stock = harness("variable", rocks, new int[] {0}, 0, true);
        var nativeHarness =
            new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
                operator, batch -> 0, Types.INT, 128, 1, 0)) {
      var config = new Configuration();
      if (rocks)
        config.setString(
            "state.backend.type",
            tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
              .getExecutionEnvironment(),
          config,
          nativeHarness.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      nativeHarness.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      nativeHarness.setup(new ArrowBatchSerializer());
      stock.open();
      feed(
          stock,
          INPUT,
          List.of(
              GenericRowData.of(1L, 10L, 2L),
              GenericRowData.of(2L, 20L, -1L),
              GenericRowData.of(1L, 5L, 3L)));
      nativeHarness.initializeState(snapshot(stock));
      nativeHarness.open();
      var suffix = List.<RowData>of(GenericRowData.of(1L, 3L, 99L), GenericRowData.of(2L, 1L, 99L));
      feed(stock, INPUT, suffix);
      push(nativeHarness, INPUT, suffix, 0);
      var expected = bytes(output, stock.extractOutputValues());
      assertFalse(expected.isEmpty());
      assertEquals(expected, bytes(output, collect(nativeHarness, output)));
      try (var restored = harness("variable", rocks, new int[] {0}, 0, true)) {
        nativeHarness.prepareSnapshotPreBarrier(2);
        restored.initializeState(
            nativeHarness
                .snapshotWithLocalState(
                    2,
                    0,
                    org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                        org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState());
        restored.open();
        var future =
            List.<RowData>of(GenericRowData.of(1L, 2L, 101L), GenericRowData.of(2L, 0L, 101L));
        feed(stock, INPUT, future);
        feed(restored, INPUT, future);
        push(nativeHarness, INPUT, future, 0);
        expected = bytes(output, stock.extractOutputValues());
        assertEquals(expected, bytes(output, restored.extractOutputValues()));
        assertEquals(expected, bytes(output, collect(nativeHarness, output)));
      }
    }
  }
}
