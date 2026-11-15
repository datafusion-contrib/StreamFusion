package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.SortedMapSerializer;
import org.apache.flink.table.runtime.typeutils.SortedMapSerializerSnapshot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.StockRetractingTopNStateContract;

class CanonicalRetractingRankValidationTest {
  @TempDir java.nio.file.Path directory;
  private static final RowType INPUT = CanonicalRetractingRankStateInteropTest.INPUT;
  private static final RowType KEY = CanonicalRetractingRankStateInteropTest.KEY;

  private CanonicalRetractingRankStateInteropTest factory() {
    var factory = new CanonicalRetractingRankStateInteropTest();
    factory.directory = directory;
    return factory;
  }

  private static JsonNode trace(boolean ttl, boolean variable) {
    return new ObjectMapper()
        .createObjectNode()
        .put("ttl_ms", ttl ? 1000 : 0)
        .put("rank_start", 1)
        .put("rank_end", 3)
        .put("output_rank", false)
        .put("variable_bound", variable);
  }

  private static RowData key(long value) {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(value)).copy();
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void multipleChunksPreserveIndependentCountersKindsEmptyListsAndAllOwnerKeys(
      boolean persistent, boolean ttl) throws Exception {
    var trace = trace(ttl, true);
    long handle = factory().create(persistent, trace, new byte[0][], 0);
    try (var view = RetractingRankStockStateTest.harness(false, persistent, false, 0)) {
      view.open();
      var backend = RetractingRankStockStateTest.backend(view);
      var data = RetractingRankStockStateTest.data(backend, ttl);
      var counts = RetractingRankStockStateTest.counts(backend, ttl);
      backend.setCurrentKey(key(1));
      var rows = new ArrayList<RowData>();
      for (int i = 0; i < 1026; i++) {
        var row = GenericRowData.of(1L, 10L, (long) i);
        row.setRowKind(RowKind.fromByteValue((byte) (i % 4)));
        rows.add(row);
      }
      data.put(key(10), ttl ? new TtlValue<>(rows, 1000) : rows);
      SortedMap<RowData, Long> map = new TreeMap<>(RetractingRankStockStateTest.comparator());
      map.put(key(10), -3L);
      map.put(key(99), Long.MAX_VALUE);
      map.put(key(77), 0L);
      counts.update(ttl ? new TtlValue<>(map, 1500) : map);
      backend.setCurrentKey(key(2));
      map = new TreeMap<>(RetractingRankStockStateTest.comparator());
      counts.update(ttl ? new TtlValue<>(map, 1200) : map);
      backend.setCurrentKey(key(3));
      data.put(key(20), ttl ? new TtlValue<>(List.<RowData>of(), 1100) : List.<RowData>of());
      backend.setCurrentKey(key(4));
      var rawBound =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "rankEnd", RetractingRankStockStateTest.wrapped(LongSerializer.INSTANCE, ttl)));
      rawBound.update(ttl ? new TtlValue<>(-7L, 500) : -7L);
      var contract = CanonicalRetractingRankStateInteropTest.contract(trace);
      contract.importState(backend, handle, persistent);
      for (String descriptor : contract.stateDescriptors().keySet())
        try (var keys = backend.getKeys(descriptor, VoidNamespace.INSTANCE)) {
          assertEquals(0, keys.count());
        }
      var callbacks = new java.util.concurrent.atomic.AtomicInteger();
      visit(
          handle,
          persistent,
          root -> {
            assertTrue(root.getRowCount() <= 1024);
            callbacks.incrementAndGet();
          });
      assertTrue(callbacks.get() >= 2);
      contract.exportState(backend, handle, persistent);
      backend.setCurrentKey(key(1));
      var actual = list(data.get(key(10)), ttl);
      assertEquals(1026, actual.size());
      for (int i = 0; i < 1026; i++) {
        assertEquals(i, actual.get(i).getLong(2));
        assertEquals(RowKind.fromByteValue((byte) (i % 4)), actual.get(i).getRowKind());
      }
      assertEquals(1000, ttl ? ((TtlValue<?>) data.get(key(10))).getLastAccessTimestamp() : 1000);
      Object value = counts.value();
      if (ttl) {
        assertEquals(1500, ((TtlValue<?>) value).getLastAccessTimestamp());
        value = ((TtlValue<?>) value).getUserValue();
      }
      map = castCounts(value);
      assertEquals(-3L, map.get(key(10)));
      assertEquals(Long.MAX_VALUE, map.get(key(99)));
      assertEquals(0L, map.get(key(77)));
      backend.setCurrentKey(key(2));
      value = counts.value();
      assertNotNull(value);
      if (ttl) value = ((TtlValue<?>) value).getUserValue();
      assertTrue(castCounts(value).isEmpty());
      backend.setCurrentKey(key(3));
      assertNotNull(data.get(key(20)));
      assertTrue(list(data.get(key(20)), ttl).isEmpty());
      backend.setCurrentKey(key(4));
      value = rawBound.value();
      if (ttl) {
        assertEquals(500, ((TtlValue<?>) value).getLastAccessTimestamp());
        value = ((TtlValue<?>) value).getUserValue();
      }
      assertEquals(-7L, value);
    } finally {
      CanonicalRetractingRankStateInteropTest.close(handle, persistent);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<RowData> list(Object value, boolean ttl) {
    return (List<RowData>) (ttl ? ((TtlValue<?>) value).getUserValue() : value);
  }

  @SuppressWarnings("unchecked")
  private static SortedMap<RowData, Long> castCounts(Object value) {
    return (SortedMap<RowData, Long>) value;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lateCorruptionIsRejectedBeforeMutationAndSourceCanBeRepairedAndRetried(boolean persistent)
      throws Exception {
    var trace = trace(false, false);
    long handle = factory().create(persistent, trace, new byte[0][], 0);
    try (var view = RetractingRankStockStateTest.harness(false, false, false, 0)) {
      view.open();
      var backend = RetractingRankStockStateTest.backend(view);
      backend.setCurrentKey(key(1));
      var data = RetractingRankStockStateTest.data(backend, false);
      var rows = new ArrayList<RowData>();
      for (int i = 0; i < 1026; i++) rows.add(GenericRowData.of(1L, 10L, (long) i));
      rows.add(GenericRowData.of(99L, 10L, 999L));
      data.put(key(10), rows);
      var contract = CanonicalRetractingRankStateInteropTest.contract(trace);
      var error =
          assertThrows(
              IllegalStateException.class, () -> contract.importState(backend, handle, persistent));
      assertTrue(error.getMessage().contains("partition or sort"), error.toString());
      var size = new java.util.concurrent.atomic.AtomicInteger();
      visit(handle, persistent, root -> size.addAndGet(root.getRowCount()));
      assertEquals(0, size.get());
      backend.setCurrentKey(key(1));
      assertEquals(1027, list(data.get(key(10)), false).size());
      rows.remove(1026);
      data.put(key(10), rows);
      contract.importState(backend, handle, persistent);
      contract.exportState(backend, handle, persistent);
      backend.setCurrentKey(key(1));
      assertEquals(1026, list(data.get(key(10)), false).size());
    } finally {
      CanonicalRetractingRankStateInteropTest.close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void memoryDeniedImportPreservesSourceForAFreshTarget(boolean persistent) throws Exception {
    var trace = trace(false, false);
    String handles = Native.liveNativeHandles();
    try (var view = RetractingRankStockStateTest.harness(false, false, false, 0)) {
      view.open();
      var backend = RetractingRankStockStateTest.backend(view);
      backend.setCurrentKey(key(1));
      var data = RetractingRankStockStateTest.data(backend, false);
      var rows = new ArrayList<RowData>();
      for (int i = 0; i < 1026; i++) rows.add(GenericRowData.of(1L, 10L, (long) i));
      data.put(key(10), rows);
      var contract = CanonicalRetractingRankStateInteropTest.contract(trace);
      long denied = factory().create(persistent, trace, new byte[0][], 0, 64);
      try {
        assertThrows(
            NativeMemoryLimitException.class,
            () -> contract.importState(backend, denied, persistent));
        backend.setCurrentKey(key(1));
        assertEquals(1026, list(data.get(key(10)), false).size());
      } finally {
        CanonicalRetractingRankStateInteropTest.close(denied, persistent);
      }
      assertEquals(handles, Native.liveNativeHandles());
      long retry = factory().create(persistent, trace, new byte[0][], 0);
      try {
        contract.importState(backend, retry, persistent);
        contract.exportState(backend, retry, persistent);
        backend.setCurrentKey(key(1));
        assertEquals(1026, list(data.get(key(10)), false).size());
      } finally {
        CanonicalRetractingRankStateInteropTest.close(retry, persistent);
      }
      assertEquals(handles, Native.liveNativeHandles());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receiverFailureKeepsOriginalExceptionAndLiveStateForAnotherExport(boolean persistent)
      throws Exception {
    var trace = trace(false, false);
    long handle = factory().create(persistent, trace, new byte[0][], 0);
    try {
      CanonicalRetractingRankStateInteropTest.push(
          handle, persistent, List.of(GenericRowData.of(1L, 10L, 1L)), false, 0);
      var failure = new IllegalStateException("receiver failed deliberately");
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  visit(
                      handle,
                      persistent,
                      root -> {
                        throw failure;
                      })));
      var count = new java.util.concurrent.atomic.AtomicInteger();
      visit(handle, persistent, root -> count.addAndGet(root.getRowCount()));
      assertEquals(4, count.get());
    } finally {
      CanonicalRetractingRankStateInteropTest.close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aPresentEmptyTargetCountMapRejectsImportAndForeignSourceStateIsRetained(boolean persistent)
      throws Exception {
    var trace = trace(false, false);
    long handle = factory().create(persistent, trace, new byte[0][], 0);
    try (var view = RetractingRankStockStateTest.harness(false, false, false, 0)) {
      view.open();
      var backend = RetractingRankStockStateTest.backend(view);
      backend.setCurrentKey(key(1));
      var counts = RetractingRankStockStateTest.counts(backend, false);
      counts.update(new TreeMap<RowData, Long>(RetractingRankStockStateTest.comparator()));
      var contract = CanonicalRetractingRankStateInteropTest.contract(trace);
      contract.importState(backend, handle, persistent);
      backend.setCurrentKey(key(2));
      counts.update(new TreeMap<RowData, Long>(RetractingRankStockStateTest.comparator()));
      assertThrows(
          IllegalStateException.class, () -> contract.importState(backend, handle, persistent));
      backend.setCurrentKey(key(2));
      assertNotNull(counts.value());
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
          .update(1L);
      var error =
          assertThrows(Exception.class, () -> contract.importState(backend, handle, persistent));
      assertTrue(error.toString().contains("descriptor is not owned"), error.toString());
      assertNotNull(counts.value());
    } finally {
      CanonicalRetractingRankStateInteropTest.close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aNullNonNullablePayloadIsRejectedBeforePrimitiveCopy(boolean persistent) throws Exception {
    var trace = trace(false, false);
    long handle = factory().create(persistent, trace, new byte[0][], 0);
    try (var view = RetractingRankStockStateTest.harness(false, false, false, 0)) {
      view.open();
      var backend = RetractingRankStockStateTest.backend(view);
      backend.setCurrentKey(key(1));
      var rowType = RowType.of(new BigIntType(), new BigIntType(false), new BigIntType());
      MapState<RowData, List<RowData>> data =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "data-state",
                  new RowDataSerializer(KEY),
                  new ListSerializer<>(new RowDataSerializer(rowType))));
      data.put(key(10), List.of(GenericRowData.of(1L, null, 7L)));
      var contract =
          new StockRetractingTopNStateContract(
              new RowDataSerializer(KEY),
              KeySelectorUtil.getRowDataSelector(
                  getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(INPUT)),
              KeySelectorUtil.getRowDataSelector(
                  getClass().getClassLoader(), new int[] {1}, InternalTypeInfo.of(INPUT)),
              KEY,
              rowType,
              RetractingRankStockStateTest.comparator(),
              false,
              false);
      assertThrows(
          IllegalStateException.class, () -> contract.importState(backend, handle, persistent));
      var size = new java.util.concurrent.atomic.AtomicInteger();
      visit(handle, persistent, root -> size.addAndGet(root.getRowCount()));
      assertEquals(0, size.get());
      backend.setCurrentKey(key(1));
      assertTrue(data.get(key(10)).get(0).isNullAt(1));
    } finally {
      CanonicalRetractingRankStateInteropTest.close(handle, persistent);
    }
  }

  @Test
  void exportDenialPreservesNativeStateAndCanRetryAfterOrdinaryCheckpointRecovery()
      throws Exception {
    var trace = trace(false, false);
    var factory = factory();
    long source = factory.create(false, trace, new byte[0][], 0);
    long stateBytes;
    try {
      CanonicalRetractingRankStateInteropTest.push(
          source, false, List.of(GenericRowData.of(1L, 10L, 1L)), false, 0);
      stateBytes = Native.topNRankerStateBytes(source);
      assertTrue(stateBytes > 0);
    } finally {
      CanonicalRetractingRankStateInteropTest.close(source, false);
    }
    byte[][] snapshots;
    long denied = factory.create(false, trace, new byte[0][], 0, stateBytes);
    try {
      CanonicalRetractingRankStateInteropTest.push(
          denied, false, List.of(GenericRowData.of(1L, 10L, 1L)), false, 0);
      assertEquals(stateBytes, Native.topNRankerStateBytes(denied));
      var called = new java.util.concurrent.atomic.AtomicBoolean();
      assertThrows(
          NativeMemoryLimitException.class, () -> visit(denied, false, root -> called.set(true)));
      assertFalse(called.get());
      assertEquals(stateBytes, Native.topNRankerStateBytes(denied));
      snapshots =
          Native.snapshotTopNRankerPartitions(
              denied, 128, new int[] {Native.STOCK_KEY_PROJECTION, -1});
      for (int i = 0; i < snapshots.length; i++)
        snapshots[i] = java.util.Arrays.copyOfRange(snapshots[i], 4, snapshots[i].length);
    } finally {
      CanonicalRetractingRankStateInteropTest.close(denied, false);
    }
    long retry = factory.create(false, trace, snapshots, 5000);
    try {
      var rows = new java.util.concurrent.atomic.AtomicInteger();
      visit(retry, false, root -> rows.addAndGet(root.getRowCount()));
      assertEquals(4, rows.get());
    } finally {
      CanonicalRetractingRankStateInteropTest.close(retry, false);
    }
  }

  @Test
  void releasedCounterSerializerRestoresWithStreamFusionClassesDenied() throws Exception {
    var serializer =
        new SortedMapSerializer<RowData, Long>(
            RetractingRankStockStateTest.comparator(),
            new RowDataSerializer(KEY),
            LongSerializer.INSTANCE);
    var snapshot = serializer.snapshotConfiguration();
    var out = new DataOutputSerializer(4096);
    snapshot.writeSnapshot(out);
    assertFalse(
        new String(out.getCopyOfBuffer(), java.nio.charset.StandardCharsets.ISO_8859_1)
            .contains("tech.streamfusion"));
    var loader =
        new ClassLoader(getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("tech.streamfusion"))
              throw new ClassNotFoundException("StreamFusion excluded: " + name);
            return super.loadClass(name, resolve);
          }
        };
    var restored = new SortedMapSerializerSnapshot<RowData, Long>();
    restored.readSnapshot(
        snapshot.getCurrentVersion(), new DataInputDeserializer(out.getCopyOfBuffer()), loader);
    var stock = restored.restoreSerializer();
    SortedMap<RowData, Long> map = new TreeMap<>(RetractingRankStockStateTest.comparator());
    map.put(key(10), -3L);
    map.put(key(20), Long.MAX_VALUE);
    out = new DataOutputSerializer(256);
    stock.serialize(map, out);
    var actual =
        (SortedMap<RowData, Long>)
            stock.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    assertEquals(-3L, actual.get(key(10)));
    assertEquals(Long.MAX_VALUE, actual.get(key(20)));
  }

  private static void visit(
      long handle, boolean persistent, java.util.function.Consumer<VectorSchemaRoot> accept) {
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
        Native.exportRetractingTopNCanonical(
            handle,
            persistent,
            schema.memoryAddress(),
            (arrayAddress, schemaAddress) -> {
              try (var array = ArrowArray.wrap(arrayAddress);
                  var exported = ArrowSchema.wrap(schemaAddress);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, exported, NativeAllocator.DICTIONARIES)) {
                accept.accept(root);
              }
            });
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }
}
