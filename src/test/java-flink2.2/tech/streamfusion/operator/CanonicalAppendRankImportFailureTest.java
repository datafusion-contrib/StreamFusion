package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTtlInteropTest.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockAppendTopNStateContract;

class CanonicalAppendRankImportFailureTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(
            "partition", "sort", "empty-list", "foreign-state", "wrong-ttl", "null-primitive")
        .flatMap(kind -> Stream.of(false, true).map(rocks -> Arguments.of(kind, rocks)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void corruptTailCannotApplyAnEarlierValidChunk(String kind, boolean rocks) throws Exception {
    long handle = create(rocks);
    try (var view = harness(false, false)) {
      view.open();
      var backend = backend(view);
      var payloadType =
          kind.equals("null-primitive")
              ? org.apache.flink.table.types.logical.RowType.of(
                  INPUT.getTypeAt(0),
                  INPUT.getTypeAt(1),
                  new org.apache.flink.table.types.logical.BigIntType(false))
              : INPUT;
      var state = lists(backend, !kind.equals("wrong-ttl"), payloadType);
      var rowSerializer = new RowDataSerializer(payloadType);
      var keySerializer = new RowDataSerializer(KEY);
      for (int key = 0; key < 1025; key++) {
        RowData row = GenericRowData.of((long) key, 10L, 3L);
        backend.setCurrentKey(keySerializer.copy(selector().getKey(row)));
        state.put(
            keySerializer.copy(sortSelector().getKey(row)),
            kind.equals("wrong-ttl")
                ? List.of(rowSerializer.copy(row))
                : new TtlValue<>(List.of(rowSerializer.copy(row)), 1000));
      }
      RowData bad =
          GenericRowData.of(
              kind.equals("partition") ? 2001L : 2000L,
              20L,
              kind.equals("null-primitive") ? null : 3L);
      RowData badKey = keySerializer.copy(selector().getKey(GenericRowData.of(2000L, 0L, 0L)));
      backend.setCurrentKey(badKey);
      RowData sort =
          keySerializer.copy(
              sortSelector().getKey(GenericRowData.of(0L, kind.equals("sort") ? 21L : 20L, 0L)));
      state.put(
          sort,
          kind.equals("wrong-ttl")
              ? List.of(bad)
              : new TtlValue<>(
                  kind.equals("empty-list")
                      ? List.of()
                      : List.of(kind.equals("null-primitive") ? bad : rowSerializer.copy(bad)),
                  1500));
      if (kind.equals("foreign-state"))
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
            .update(1L);
      var ordered = orderedKeys(backend, badKey);
      var contract =
          new StockAppendTopNStateContract(
              new RowDataSerializer(KEY),
              selector(),
              sortSelector(),
              KEY,
              payloadType,
              new int[] {1},
              false,
              false,
              true);
      var failure =
          assertThrows(Exception.class, () -> contract.importState(ordered, handle, rocks));
      String expected =
          kind.equals("partition")
              ? "partition key"
              : kind.equals("sort")
                  ? "sort key"
                  : kind.equals("empty-list")
                      ? "empty retained list"
                      : kind.equals("foreign-state")
                          ? "descriptor is not owned"
                          : kind.equals("null-primitive") ? "NULL non-null" : "TtlValue";
      assertTrue(failure.toString().contains(expected), failure.toString());
      assertEquals(0, nativeRows(handle, rocks));
      backend.setCurrentKey(badKey);
      assertNotNull(state.get(sort));
      try (var retainedKeys = backend.getKeys("data-state-with-append", VoidNamespace.INSTANCE)) {
        assertEquals(1026, retainedKeys.count());
      }
      // A fresh retry must report the same corruption, rather than consume or clear the source.
      var retry = assertThrows(Exception.class, () -> contract.importState(ordered, handle, rocks));
      assertEquals(failure.getClass(), retry.getClass());
      assertEquals(failure.getMessage(), retry.getMessage());
      assertEquals(0, nativeRows(handle, rocks));
      backend.setCurrentKey(keySerializer.copy(selector().getKey(GenericRowData.of(0L, 0L, 0L))));
      assertNotNull(
          state.get(keySerializer.copy(sortSelector().getKey(GenericRowData.of(0L, 10L, 0L)))));
    } finally {
      if (rocks) Native.closeRocksDBTopNRanker(handle);
      else Native.closeTopNRanker(handle);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static MapState<RowData, Object> lists(
      CheckpointableKeyedStateBackend<RowData> backend,
      boolean ttl,
      org.apache.flink.table.types.logical.RowType type)
      throws Exception {
    var value = new ListSerializer<>(new RowDataSerializer(type));
    org.apache.flink.api.common.typeutils.TypeSerializer serializer =
        ttl ? new TtlSerializer<>(LongSerializer.INSTANCE, value) : value;
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<RowData, Object>(
            "data-state-with-append", new RowDataSerializer(KEY), serializer));
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> orderedKeys(
      CheckpointableKeyedStateBackend<RowData> backend, RowData tail) {
    return (CheckpointableKeyedStateBackend<RowData>)
        Proxy.newProxyInstance(
            CanonicalAppendRankImportFailureTest.class.getClassLoader(),
            new Class<?>[] {CheckpointableKeyedStateBackend.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getKeys") && args[0].equals("data-state-with-append"))
                return Stream.concat(
                    java.util.stream.IntStream.range(0, 1025)
                        .mapToObj(
                            key -> {
                              try {
                                return new RowDataSerializer(KEY)
                                    .copy(selector().getKey(GenericRowData.of((long) key, 0L, 0L)));
                              } catch (Exception failure) {
                                throw new IllegalStateException(failure);
                              }
                            }),
                    Stream.of(tail));
              try {
                return method.invoke(backend, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
            });
  }

  private long create(boolean rocks) {
    if (!rocks)
      return Native.createTopNRanker(
          new int[] {0},
          new int[] {Native.STOCK_KEY_PROJECTION, -1},
          new int[] {1},
          new int[] {1},
          new int[] {1},
          0,
          3,
          -1,
          true,
          false,
          true,
          false,
          1000,
          1 << 24);
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
        return Native.createRocksDBTopNRanker(
            new int[] {0},
            new int[] {Native.STOCK_KEY_PROJECTION, -1},
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            3,
            -1,
            true,
            false,
            true,
            false,
            1000,
            0,
            1 << 24,
            schema.memoryAddress(),
            directory.resolve("rocks").toString(),
            128,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            0,
            new String[0],
            new String[0],
            0,
            127,
            false,
            new byte[0][]);
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  private static int nativeRows(long handle, boolean rocks) {
    var count = new AtomicInteger();
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
        Native.exportAppendTopNCanonical(
            handle,
            rocks,
            schema.memoryAddress(),
            (arrayAddress, schemaAddress) -> {
              try (var array = ArrowArray.wrap(arrayAddress);
                  var exported = ArrowSchema.wrap(schemaAddress);
                  VectorSchemaRoot root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, exported, NativeAllocator.DICTIONARIES)) {
                count.addAndGet(root.getRowCount());
              }
            });
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
    return count.get();
  }
}
