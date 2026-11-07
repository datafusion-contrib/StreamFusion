package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.backend;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockBufferedDedupStateContract;

class CanonicalBufferedDedupMatrixTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(false, true)
                    .flatMap(
                        ttl ->
                            Stream.of("nullable", "not-null", "keyless")
                                .flatMap(
                                    shape ->
                                        (rocks
                                                ? Stream.of("none", "partitions", "sst")
                                                : Stream.of("none", "partitions"))
                                            .map(
                                                recovery ->
                                                    Arguments.of(rocks, ttl, shape, recovery)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void compactTimestampCandidatesAndMarkersContinueThroughBothEnginesAndNativeRecovery(
      boolean rocks, boolean ttl, String shape, String recovery) throws Exception {
    RowType type =
        RowType.of(
            new BigIntType(!shape.equals("not-null")),
            new BigIntType(),
            new TimestampType(false, 3));
    int[] keys = shape.equals("keyless") ? new int[0] : new int[] {0};
    RowType keyType = keys.length == 0 ? RowType.of() : RowType.of(type.getTypeAt(0));
    long retention = ttl ? 1000 : 0;
    var selector = selector(type, keys);
    var contract =
        new StockBufferedDedupStateContract(
            new RowDataSerializer(keyType), selector, keyType, type, keys, 2, retention);
    long handle = create(rocks, type, keys, retention, "initial", new byte[0][], null, null);
    Long third = shape.equals("not-null") ? 3L : null;
    try (var stock = harness(type, keys, retention, true);
        var view = harness(type, keys, 0, false)) {
      stock.open();
      time(stock, 1500);
      stockPush(
          stock,
          type,
          List.of(row(1L, 10, 1000), row(2L, 20, 2000), row(third, 30, 3000), row(1L, 11, 900)));
      stock.processWatermark(new Watermark(1000));
      assertEquals(List.of("1|11|900"), values(stock.extractOutputValues()));
      view.initializeState(snapshot(stock));
      view.open();
      time(view, 10000);
      contract.importState(backend(view), handle, rocks);
      var suffix =
          List.of(row(1L, 12, 2200), row(2L, 25, 1800), row(third, 35, 2800), row(4L, 40, 4000));
      time(stock, 2200);
      stockPush(stock, type, suffix);
      stock.processWatermark(new Watermark(3000));
      push(handle, rocks, type, 2200, suffix);
      assertEquals(values(stock.extractOutputValues()), flush(handle, rocks, type, 3000, 2200));
      if (!recovery.equals("none")) {
        long restored = recover(handle, rocks, type, keys, retention, recovery);
        close(handle, rocks);
        handle = restored;
      }
      contract.exportState(backend(view), handle, rocks);
      try (var restored = harness(type, keys, retention, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        time(restored, 2600);
        time(stock, 2600);
        var future = List.of(row(1L, 13, 3500), row(2L, 26, 3600), row(4L, 45, 3900));
        stockPush(stock, type, future);
        stock.processWatermark(new Watermark(5000));
        stockPush(restored, type, future);
        restored.processWatermark(new Watermark(5000));
        push(handle, rocks, type, 2600, future);
        var expected = values(stock.extractOutputValues());
        assertEquals(expected, values(restored.extractOutputValues()));
        assertEquals(expected, flush(handle, rocks, type, 5000, 2600));
        var oracle = new ArrayList<String>();
        if (ttl) oracle.add("1|13|3500");
        if (!shape.equals("keyless")) oracle.add("4|45|3900");
        assertEquals(
            oracle.stream().sorted().collect(java.util.stream.Collectors.toList()), expected);
      }
    } finally {
      close(handle, rocks);
    }
  }

  private static int[] descriptor(int[] keys) {
    return keys.length == 0
        ? new int[] {Native.STOCK_KEY_PROJECTION}
        : new int[] {Native.STOCK_KEY_PROJECTION, -1};
  }

  private long create(
      boolean rocks,
      RowType type,
      int[] keys,
      long retention,
      String name,
      byte[][] partitions,
      String source,
      String token) {
    if (!rocks)
      return partitions.length == 0
          ? Native.createKeepFirstDeduplicator(keys, descriptor(keys), 2, retention, 1 << 24)
          : Native.restoreKeepFirstDeduplicatorPartitions(
              keys, descriptor(keys), 2, retention, 2200, partitions, 1 << 24);
    int maximum = keys.length == 0 ? 1 : 128;
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(type),
            NativeAllocator.DICTIONARIES,
            schema);
        return Native.createRocksDBKeepFirstDeduplicator(
            keys,
            descriptor(keys),
            2,
            retention,
            2200,
            1 << 24,
            schema.memoryAddress(),
            directory.resolve(name).toString(),
            maximum,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            0,
            source == null ? new String[0] : new String[] {source},
            token == null ? new String[0] : new String[] {token},
            0,
            maximum - 1,
            source != null,
            partitions);
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  private long recover(
      long handle, boolean rocks, RowType type, int[] keys, long retention, String recovery)
      throws Exception {
    if (recovery.equals("sst")) {
      String source = directory.resolve("checkpoint").toString();
      var manifest = Native.checkpointRocksDBKeepFirstDeduplicator(handle, source);
      assertTrue(manifest.length > 0);
      return create(true, type, keys, retention, "restored", new byte[0][], source, manifest[0]);
    }
    var partitions =
        rocks
            ? Native.snapshotRocksDBKeepFirstDeduplicatorPartitions(handle)
            : Native.snapshotKeepFirstDeduplicatorPartitions(
                handle, keys.length == 0 ? 1 : 128, descriptor(keys));
    assertTrue(
        partitions.length > 0, "Imported marker/pending state must reach native checkpoints");
    return create(
        rocks,
        type,
        keys,
        retention,
        "restored",
        checkpointPayloads(partitions, keys.length == 0 ? 1 : 128),
        null,
        null);
  }

  private static byte[][] checkpointPayloads(byte[][] partitions, int maximum) throws Exception {
    try (var registry = new org.apache.flink.core.fs.CloseableRegistry()) {
      var context =
          new org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl(
              1,
              1,
              new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(8 << 20),
              new org.apache.flink.runtime.state.KeyGroupRange(0, maximum - 1),
              registry);
      RawKeyedState.snapshotPartitions(context, partitions);
      var future = context.getKeyedStateStreamFuture();
      future.run();
      var handle =
          (org.apache.flink.runtime.state.KeyGroupsStateHandle)
              future.get().getJobManagerOwnedSnapshot();
      var providers =
          new ArrayList<org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider>();
      for (var group : handle.getGroupRangeOffsets()) {
        if (group.f1
            == org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream.NO_OFFSET_SET)
          continue;
        var input = handle.openInputStream();
        registry.registerCloseable(input);
        input.seek(group.f1);
        providers.add(
            new org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider(
                input, group.f0));
      }
      var unused =
          (org.apache.flink.api.common.state.KeyedStateStore)
              java.lang.reflect.Proxy.newProxyInstance(
                  CanonicalBufferedDedupMatrixTest.class.getClassLoader(),
                  new Class<?>[] {org.apache.flink.api.common.state.KeyedStateStore.class},
                  (proxy, method, args) -> {
                    throw new UnsupportedOperationException(method.getName());
                  });
      var restore =
          new org.apache.flink.runtime.state.StateInitializationContextImpl(
              1L, null, unused, providers, null);
      return RawKeyedState.restore(restore).toArray(new byte[0][]);
    }
  }

  private static void close(long handle, boolean rocks) {
    if (rocks) Native.closeRocksDBKeepFirstDeduplicator(handle);
    else Native.closeKeepFirstDeduplicator(handle);
  }

  private static RowDataKeySelector selector(RowType type, int[] keys) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalBufferedDedupMatrixTest.class.getClassLoader(), keys, InternalTypeInfo.of(type));
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      RowType type, int[] keys, long retention, boolean stock) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator =
        stock
            ? new KeyedProcessOperator<>(
                new RowTimeDeduplicateKeepFirstRowFunction(InternalTypeInfo.of(type), retention, 2))
            : new Projection();
    var selector = selector(type, keys);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), keys.length == 0 ? 1 : 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(type));
    return harness;
  }

  private static void time(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, long now)
      throws Exception {
    harness.setProcessingTime(now);
    harness.setStateTtlProcessingTime(now);
  }

  private static RowData row(Long key, long value, long time) {
    return GenericRowData.of(key, value, TimestampData.fromEpochMillis(time));
  }

  private static void stockPush(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      RowType type,
      List<RowData> rows)
      throws Exception {
    harness.getOutput().clear();
    var serializer = new RowDataSerializer(type);
    for (var row : rows)
      harness.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
  }

  private static List<String> values(List<RowData> rows) {
    return rows.stream()
        .map(
            row ->
                (row.isNullAt(0) ? "null" : Long.toString(row.getLong(0)))
                    + "|"
                    + row.getLong(1)
                    + "|"
                    + row.getTimestamp(2, 3).getMillisecond())
        .sorted()
        .collect(java.util.stream.Collectors.toList());
  }

  private static void push(
      long handle, boolean rocks, RowType type, long time, List<RowData> rows) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (rocks)
        Native.pushRocksDBKeepFirstDeduplicator(
            handle, array.memoryAddress(), schema.memoryAddress(), time);
      else
        Native.pushKeepFirstDeduplicator(
            handle, array.memoryAddress(), schema.memoryAddress(), time);
    }
  }

  private static List<String> flush(
      long handle, boolean rocks, RowType type, long watermark, long time) {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      if (rocks)
        Native.flushRocksDBKeepFirstDeduplicator(
            handle, watermark, time, array.memoryAddress(), schema.memoryAddress());
      else
        Native.flushKeepFirstDeduplicator(
            handle, watermark, time, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot root =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
        if (root.getRowCount() == 0) return List.of();
        var reader = ArrowConversion.createArrowReader(root, type);
        var serializer = new RowDataSerializer(type);
        var rows = new ArrayList<RowData>();
        for (int row = 0; row < root.getRowCount(); row++)
          rows.add(serializer.copy(reader.read(row)));
        return values(rows);
      }
    }
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> record) {}
  }
}
