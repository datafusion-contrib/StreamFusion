package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.DedupCanonicalTestSupport.close;
import static tech.streamfusion.operator.DedupCanonicalTestSupport.nativePush;
import static tech.streamfusion.operator.DedupCanonicalTestSupport.row;
import static tech.streamfusion.operator.DedupCanonicalTestSupport.stockPush;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.backend;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.StockEmptyDedupStateContract;

class CanonicalBareUpdateDedupInteropTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(false, true)
                    .flatMap(
                        ttl ->
                            Stream.of("nullable", "not-null", "keyless")
                                .map(shape -> Arguments.of("proc-last", rocks, ttl, shape))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void selectedRowsMarkersAndKindsContinueAcrossBothEngineSwitches(
      String mode, boolean rocks, boolean ttl, String shape) throws Exception {
    RowType type =
        RowType.of(
            new BigIntType(!shape.equals("not-null")), new BigIntType(), new TimestampType(3));
    int[] keys = shape.equals("keyless") ? new int[0] : new int[] {0};
    RowType keyType = keys.length == 0 ? RowType.of() : RowType.of(type.getTypeAt(0));
    boolean rowtime = mode.startsWith("row");
    boolean first = mode.endsWith("first");
    long retention = ttl ? 1000 : 0;
    long handle = create(rocks, keys, rowtime, first, retention);
    var prefix =
        List.<RowData>of(
            row(1L, 10, 1000),
            row(1L, 10, 1000),
            row(2L, 20, 2000),
            row(1L, 15, 1500),
            row(shape.equals("not-null") ? 3L : null, 30, 3000));
    var suffix =
        List.<RowData>of(row(1L, 15, 1500), row(1L, 9, 900), row(2L, 25, 2500), row(4L, 40, 4000));
    var future = List.<RowData>of(row(1L, 8, 800), row(4L, 45, 4500));
    try (var stock = harness(type, keys, mode, retention, true);
        var view = harness(type, keys, mode, 0, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1500);
      stockPush(stock, type, prefix);
      view.initializeState(snapshot(stock));
      view.open();
      view.setStateTtlProcessingTime(10000);
      var contract =
          new StockEmptyDedupStateContract(
              new RowDataSerializer(keyType), keyType, type, retention);
      contract.importState(backend(view), handle, rocks);
      stock.setStateTtlProcessingTime(1800);
      assertEquals(
          stockPush(stock, type, suffix),
          nativePush(handle, rocks, type, suffix, 1800, first && !rowtime));
      contract.exportState(backend(view), handle, rocks);
      try (var retained = backend(view).getKeys("deduplicate-state", VoidNamespace.INSTANCE)) {
        assertEquals(0, retained.count(), "Stock bare-update mode must remain empty");
      }
      try (var restored = harness(type, keys, mode, retention, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        restored.setStateTtlProcessingTime(1900);
        stock.setStateTtlProcessingTime(1900);
        var expected = stockPush(stock, type, future);
        assertEquals(expected, stockPush(restored, type, future));
        assertEquals(expected, nativePush(handle, rocks, type, future, 1900, first && !rowtime));
      }
    } finally {
      if (rocks) Native.closeRocksDBKeepLastDeduplicator(handle);
      else Native.closeKeepLastDeduplicator(handle);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void rejectsUnexpectedStockRowsWithoutMutatingEitherEngine(boolean rocks, boolean ttl)
      throws Exception {
    var type = RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3));
    var keyType = RowType.of(new BigIntType());
    long retention = ttl ? 1000 : 0;
    long handle = create(rocks, new int[] {0}, false, false, retention);
    try (var view = harness(type, new int[] {0}, "proc-last", 0, false);
        var observed = harness(type, new int[] {0}, "proc-last", 0, false)) {
      view.open();
      observed.open();
      var backend = backend(view);
      @SuppressWarnings("unchecked")
      var serializer =
          (org.apache.flink.api.common.typeutils.TypeSerializer<Object>)
              (org.apache.flink.api.common.typeutils.TypeSerializer<?>)
                  (ttl
                      ? new org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer<>(
                          org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
                          new RowDataSerializer(type))
                      : new RowDataSerializer(type));
      var state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "deduplicate-state", serializer));
      backend.setCurrentKey(
          new RowDataSerializer(keyType).toBinaryRow(GenericRowData.of(1L)).copy());
      state.update(
          ttl
              ? new org.apache.flink.runtime.state.ttl.TtlValue<>(row(1L, 10, 1000), 1500)
              : row(1L, 10, 1000));
      var contract =
          new StockEmptyDedupStateContract(
              new RowDataSerializer(keyType), keyType, type, retention);
      assertThrows(IllegalStateException.class, () -> contract.importState(backend, handle, rocks));
      Object retained = state.value();
      RowData value =
          (RowData)
              (ttl
                  ? ((org.apache.flink.runtime.state.ttl.TtlValue<?>) retained).getUserValue()
                  : retained);
      assertEquals(10, value.getLong(1));
      new tech.streamfusion.state.StockDedupStateContract(
              new RowDataSerializer(keyType), keyType, type, false, -1, retention)
          .exportState(backend(observed), handle, rocks);
      try (var keys = backend(observed).getKeys("deduplicate-state", VoidNamespace.INSTANCE)) {
        assertEquals(0, keys.count(), "Rejected rows must not enter native state");
      }
    } finally {
      close(handle, rocks);
    }
  }

  private long create(boolean rocks, int[] keys, boolean rowtime, boolean first, long retention) {
    return DedupCanonicalTestSupport.create(
        directory, rocks, keys, rowtime, first, retention, false, false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      RowType type, int[] keys, String mode, long retention, boolean stock) throws Exception {
    return DedupCanonicalTestSupport.harness(type, keys, mode, retention, stock, false, false);
  }
}
