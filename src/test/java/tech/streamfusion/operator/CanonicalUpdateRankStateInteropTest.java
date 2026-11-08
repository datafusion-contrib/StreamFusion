package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalUpdateRankStateInteropTest {
  @TempDir java.nio.file.Path directory;
  static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final RowType ROW_KEY = RowType.of(new BigIntType(), new BigIntType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void updateImportFailureSurvivesArrowRelease(boolean persistent) throws Exception {
    long handle = create(persistent, false);
    try {
      try (var root =
              RowDataArrowConverter.write(
                  java.util.List.of(GenericRowData.of(1L)), KEY, NativeAllocator.SHARED);
          var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
        assertThrows(
            tech.streamfusion.NativeException.class,
            () ->
                Native.importUpdatingTopNCanonical(
                    handle, persistent, array.memoryAddress(), schema.memoryAddress()));
      }
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void tiedRowKeysAndKindsSurviveMultipleTransferChunksAndCanonicalReexport(boolean persistent)
      throws Exception {
    long handle = create(persistent, false);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      var data = data(backend);
      for (int index = 1025; index >= 0; index--) {
        var row = GenericRowData.of(1L, 10L, (long) index);
        row.setRowKind(RowKind.fromByteValue((byte) (index % 4)));
        data.put(rowKey(index), Tuple2.of(row, index + 1));
      }
      contract(false).importState(backend, handle, persistent);
      contract(false).exportState(backend, handle, persistent);
      var snapshot =
          view.snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = harness()) {
        restored.initializeState(snapshot);
        restored.open();
        var saved = backend(restored);
        saved.setCurrentKey(key(1));
        var tuples = data(saved);
        int count = 0;
        for (var ignored : tuples.entries()) count++;
        assertEquals(1026, count);
        for (int index = 0; index < 1026; index++) {
          var tuple = tuples.get(rowKey(index));
          assertNotNull(tuple);
          assertEquals(index + 1, tuple.f1);
          assertEquals(index, tuple.f0.getLong(2));
          assertEquals(RowKind.fromByteValue((byte) (index % 4)), tuple.f0.getRowKind());
        }
      }
      var metadata = new DataOutputSerializer(4096);
      tupleSerializer().snapshotConfiguration().writeSnapshot(metadata);
      assertFalse(
          new String(metadata.getCopyOfBuffer(), java.nio.charset.StandardCharsets.ISO_8859_1)
              .contains("tech.streamfusion"));
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"row_key", "duplicate_position", "missing_position"})
  void malformedUniqueKeysAndTiePositionsAreRejectedBeforeImport(String mode) throws Exception {
    long handle = create(false, false);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      data(backend)
          .put(
              rowKey(mode.equals("row_key") ? 99 : 1),
              Tuple2.of(GenericRowData.of(1L, 10L, 1L), mode.equals("missing_position") ? 2 : 1));
      if (mode.equals("duplicate_position"))
        data(backend).put(rowKey(2), Tuple2.of(GenericRowData.of(1L, 10L, 2L), 1));
      assertThrows(
          IllegalStateException.class, () -> contract(false).importState(backend, handle, false));
      assertEquals(0, Native.topNRankerStateBytes(handle));
    } finally {
      close(handle, false);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void updateFastTop1UsesTheStockValueDescriptor(boolean persistent) throws Exception {
    long handle = create(persistent, true);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      ValueState<RowData> winner =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("Top1-Rank-State", new RowDataSerializer(INPUT)));
      var row = GenericRowData.of(1L, 10L, 7L);
      row.setRowKind(RowKind.UPDATE_AFTER);
      winner.update(row);
      contract(true).importState(backend, handle, persistent);
      contract(true).exportState(backend, handle, persistent);
      assertEquals(7, winner.value().getLong(2));
      assertEquals(RowKind.UPDATE_AFTER, winner.value().getRowKind());
      try (var keys = backend.getKeys("data-state-with-update", VoidNamespace.INSTANCE)) {
        assertEquals(0, keys.count());
      }
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nativeOverflowStateAndContinuationMatchTheReleasedUpdateFunction(boolean persistent)
      throws Exception {
    long handle = create(persistent, false, 3);
    try (var stock = harness(true)) {
      stock.open();
      var prefix =
          java.util.List.<RowData>of(
              GenericRowData.of(1L, 10L, 3L),
              GenericRowData.of(1L, 20L, 4L),
              GenericRowData.of(1L, 30L, 5L),
              GenericRowData.of(1L, 15L, 2L));
      for (var row : prefix)
        stock.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(row)));
      var snapshot =
          stock
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var imported = harness()) {
        imported.initializeState(snapshot);
        imported.open();
        var backend = backend(imported);
        backend.setCurrentKey(key(1));
        int count = 0;
        for (var ignored : data(backend).entries()) count++;
        assertEquals(4, count);
        contract(false).importState(backend, handle, persistent);
        contract(false).exportState(backend, handle, persistent);
        count = 0;
        for (var ignored : data(backend).entries()) count++;
        assertEquals(4, count);
      }
      stock.getOutput().clear();
      var changed = GenericRowData.of(1L, 7L, 5L);
      changed.setRowKind(RowKind.UPDATE_AFTER);
      stock.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(changed)));
      var expected =
          stock.extractOutputValues().stream()
              .map(CanonicalUpdateRankStateInteropTest::value)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(expected, push(handle, persistent, java.util.List.of(changed), 0));
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aLateInvalidTiePositionLeavesTheEntireSourceAndTargetAvailableForRetry(boolean persistent)
      throws Exception {
    long handle = create(persistent, false);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      var rows = data(backend);
      for (int index = 0; index < 1026; index++)
        rows.put(
            rowKey(index),
            Tuple2.of(GenericRowData.of(1L, 10L, (long) index), index == 1025 ? 1027 : index + 1));
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> contract(false).importState(backend, handle, persistent));
      assertEquals("Nonconsecutive update rank inner positions", failure.getMessage());
      assertEquals(0, nativeRows(handle, persistent));
      assertEquals(1026, size(rows));
      assertEquals(1027, rows.get(rowKey(1025)).f1);
      rows.put(rowKey(1025), Tuple2.of(GenericRowData.of(1L, 10L, 1025L), 1026));
      contract(false).importState(backend, handle, persistent);
      assertEquals(0, size(rows));
      assertEquals(1026, nativeRows(handle, persistent));
      contract(false).exportState(backend, handle, persistent);
      assertEquals(1026, size(rows));
      assertEquals(1026, rows.get(rowKey(1025)).f1);
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void populatedTargetIsRejectedWithoutReplacingSourceOrNativeState(boolean persistent)
      throws Exception {
    long handle = create(persistent, false);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      var rows = data(backend);
      rows.put(rowKey(7), Tuple2.of(GenericRowData.of(1L, 10L, 7L), 1));
      contract(false).importState(backend, handle, persistent);
      assertEquals(0, size(rows));
      rows.put(rowKey(8), Tuple2.of(GenericRowData.of(1L, 20L, 8L), 1));
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> contract(false).importState(backend, handle, persistent));
      assertTrue(failure.getMessage().contains("empty target"), failure.toString());
      assertEquals(1, size(rows));
      assertNotNull(rows.get(rowKey(8)));
      assertEquals(1, nativeRows(handle, persistent));
      contract(false).exportState(backend, handle, persistent);
      assertEquals(1, size(rows));
      assertNotNull(rows.get(rowKey(7)));
      assertNull(rows.get(rowKey(8)));
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receiverFailureReleasesArrowBuffersAndLeavesStateAvailableForRetry(boolean persistent)
      throws Exception {
    long handle = create(persistent, false);
    try (var view = harness()) {
      view.open();
      var input = new java.util.ArrayList<RowData>();
      for (long id = 0; id < 1026; id++) input.add(GenericRowData.of(1L, 10L, id));
      push(handle, persistent, input, 0);
      var marker = new IllegalStateException("update receiver marker");
      long before = NativeAllocator.SHARED.getAllocatedMemory();
      assertSame(
          marker,
          assertThrows(
              IllegalStateException.class,
              () ->
                  visitNative(
                      handle,
                      persistent,
                      root -> {
                        assertTrue(root.getRowCount() <= 1024);
                        throw marker;
                      })));
      assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(1026, nativeRows(handle, persistent));
      contract(false).exportState(backend(view), handle, persistent);
      backend(view).setCurrentKey(key(1));
      assertEquals(1026, size(data(backend(view))));
      assertEquals(1026, data(backend(view)).get(rowKey(1025)).f1);
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void foreignStateIsRejectedWithoutConsumingAnySourceRows(boolean persistent) throws Exception {
    long handle = create(persistent, false);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      data(backend).put(rowKey(7), Tuple2.of(GenericRowData.of(1L, 10L, 7L), 1));
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "foreign", org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE))
          .update(1L);
      var failure =
          assertThrows(
              Exception.class, () -> contract(false).importState(backend, handle, persistent));
      assertTrue(failure.toString().contains("descriptor is not owned"), failure.toString());
      assertEquals(0, nativeRows(handle, persistent));
      backend.setCurrentKey(key(1));
      assertNotNull(data(backend).get(rowKey(7)));
      var retry =
          assertThrows(
              Exception.class, () -> contract(false).importState(backend, handle, persistent));
      assertEquals(failure.getClass(), retry.getClass());
      assertEquals(failure.getMessage(), retry.getMessage());
      assertEquals(0, nativeRows(handle, persistent));
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aNullNonNullablePrimitiveCannotBecomeAZeroDuringImport(boolean persistent) throws Exception {
    long handle = create(persistent, false);
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      var nonNullType = RowType.of(new BigIntType(), new BigIntType(false), new BigIntType());
      TypeSerializer<Tuple2<RowData, Integer>> tuples =
          new TupleSerializer(
              (Class) Tuple2.class,
              new TypeSerializer<?>[] {new RowDataSerializer(nonNullType), IntSerializer.INSTANCE});
      MapState<RowData, Tuple2<RowData, Integer>> rows =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "data-state-with-update", new RowDataSerializer(ROW_KEY), tuples));
      backend.setCurrentKey(key(1));
      rows.put(rowKey(7), Tuple2.of(GenericRowData.of(1L, null, 7L), 1));
      var contract =
          new StockUpdatingTopNStateContract(
              new RowDataSerializer(KEY),
              selector(0),
              selector(0, 2),
              selector(1),
              KEY,
              nonNullType,
              false);
      var failure =
          assertThrows(
              IllegalStateException.class, () -> contract.importState(backend, handle, persistent));
      assertTrue(failure.getMessage().contains("NULL non-null"), failure.toString());
      assertEquals(0, nativeRows(handle, persistent));
      backend.setCurrentKey(key(1));
      assertTrue(rows.get(rowKey(7)).f0.isNullAt(1));
      rows.put(rowKey(7), Tuple2.of(GenericRowData.of(1L, 10L, 7L), 1));
      contract.importState(backend, handle, persistent);
      assertEquals(1, nativeRows(handle, persistent));
      assertEquals(0, size(rows));
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aMemoryDeniedImportRetainsSourceForAFreshTarget(boolean persistent) throws Exception {
    String handles = Native.liveNativeHandles();
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      var rows = data(backend);
      for (int index = 0; index < 1026; index++)
        rows.put(rowKey(index), Tuple2.of(GenericRowData.of(1L, 10L, (long) index), index + 1));
      long denied = create(persistent, false, 2000, 64);
      try {
        assertThrows(
            tech.streamfusion.NativeMemoryLimitException.class,
            () -> contract(false).importState(backend, denied, persistent));
        backend.setCurrentKey(key(1));
        assertEquals(1026, size(rows));
      } finally {
        close(denied, persistent);
      }
      assertEquals(handles, Native.liveNativeHandles());
      long retry = create(persistent, false, 2000);
      try {
        contract(false).importState(backend, retry, persistent);
        assertEquals(1026, nativeRows(retry, persistent));
        backend.setCurrentKey(key(1));
        assertEquals(0, size(rows));
      } finally {
        close(retry, persistent);
      }
      assertEquals(handles, Native.liveNativeHandles());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ttlDisabledCheckpointMigrationClockIsOmittedFromStockCanonicalState(boolean persistent)
      throws Exception {
    var prefix =
        java.util.List.<RowData>of(
            GenericRowData.of(1L, 10L, 3L),
            GenericRowData.of(1L, 20L, 4L),
            GenericRowData.of(1L, 30L, 5L),
            GenericRowData.of(1L, 15L, 2L));
    long source = create(false, false, 3);
    byte[][] checkpoint;
    try {
      push(source, false, prefix, 1000);
      checkpoint =
          Native.snapshotTopNRankerPartitions(
              source, 128, new int[] {Native.STOCK_KEY_PROJECTION, -1});
      assertEquals(1, checkpoint.length);
      int keyGroup = java.nio.ByteBuffer.wrap(checkpoint[0]).getInt();
      assertTrue(keyGroup >= 0 && keyGroup < 128);
      checkpoint[0] =
          java.util.Arrays.copyOfRange(checkpoint[0], Integer.BYTES, checkpoint[0].length);
    } finally {
      close(source, false);
    }
    long restored = create(persistent, false, 3, 1 << 24, 5000, checkpoint);
    try (var view = harness();
        var stock = harness(true);
        var resumed = harness(true)) {
      view.open();
      stock.open();
      for (var row : prefix)
        stock.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(row)));
      stock.getOutput().clear();
      var transferred = new java.util.concurrent.atomic.AtomicInteger();
      visitNative(
          restored,
          persistent,
          root -> {
            var times =
                (org.apache.arrow.vector.BigIntVector) root.getVector(INPUT.getFieldCount() + 3);
            for (int row = 0; row < root.getRowCount(); row++) {
              assertEquals(
                  0,
                  times.get(row),
                  "TTL-disabled transfer must omit the raw-restore migration clock");
              transferred.incrementAndGet();
            }
          });
      assertEquals(4, transferred.get());
      contract(false).exportState(backend(view), restored, persistent);
      resumed.initializeState(
          view.snapshotWithLocalState(
                  1, 5000, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState());
      resumed.open();
      var future = GenericRowData.of(1L, 7L, 5L);
      future.setRowKind(RowKind.UPDATE_AFTER);
      stock.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(future)));
      resumed.processElement(new StreamRecord<>(new RowDataSerializer(INPUT).copy(future)));
      var expected =
          stock.extractOutputValues().stream()
              .map(CanonicalUpdateRankStateInteropTest::value)
              .collect(java.util.stream.Collectors.toList());
      assertFalse(expected.isEmpty());
      assertEquals(
          expected,
          resumed.extractOutputValues().stream()
              .map(CanonicalUpdateRankStateInteropTest::value)
              .collect(java.util.stream.Collectors.toList()));
      assertEquals(expected, push(restored, persistent, java.util.List.of(future), 6000));
    } finally {
      close(restored, persistent);
    }
  }

  private static int size(MapState<RowData, Tuple2<RowData, Integer>> rows) throws Exception {
    int count = 0;
    for (var ignored : rows.entries()) count++;
    return count;
  }

  private static int nativeRows(long handle, boolean persistent) {
    var count = new java.util.concurrent.atomic.AtomicInteger();
    visitNative(handle, persistent, root -> count.addAndGet(root.getRowCount()));
    return count.get();
  }

  private static void visitNative(
      long handle, boolean persistent, java.util.function.Consumer<VectorSchemaRoot> receiver) {
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
        Native.exportUpdatingTopNCanonical(
            handle,
            persistent,
            schema.memoryAddress(),
            (arrayAddress, schemaAddress) -> {
              try (var array = ArrowArray.wrap(arrayAddress);
                  var exported = ArrowSchema.wrap(schemaAddress);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, exported, NativeAllocator.DICTIONARIES)) {
                receiver.accept(root);
              }
            });
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  private static java.util.List<String> push(
      long handle, boolean persistent, java.util.List<RowData> rows, long time) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED, true);
        ArrowArray in = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema inSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        ArrowArray out = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, in, inSchema);
      if (persistent)
        Native.pushRocksDBTopNRanker(
            handle,
            in.memoryAddress(),
            inSchema.memoryAddress(),
            time,
            out.memoryAddress(),
            outSchema.memoryAddress());
      else
        Native.pushTopNRanker(
            handle,
            in.memoryAddress(),
            inSchema.memoryAddress(),
            time,
            out.memoryAddress(),
            outSchema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, out, outSchema, NativeAllocator.DICTIONARIES)) {
        if (result.getRowCount() == 0) return java.util.List.of();
        var reader = ArrowConversion.createArrowReader(result, OUTPUT);
        var kinds =
            (org.apache.arrow.vector.TinyIntVector)
                result.getVector(result.getFieldVectors().size() - 1);
        var values = new java.util.ArrayList<String>();
        for (int index = 0; index < result.getRowCount(); index++) {
          var row = reader.read(index);
          row.setRowKind(org.apache.flink.types.RowKind.fromByteValue(kinds.get(index)));
          values.add(value(row));
        }
        return values;
      }
    }
  }

  private static String value(RowData row) {
    return row.getRowKind().toByteValue()
        + ":"
        + row.getLong(0)
        + ":"
        + row.getLong(1)
        + ":"
        + row.getLong(2)
        + ":"
        + row.getLong(3);
  }

  static StockUpdatingTopNStateContract contract(boolean top1) {
    return new StockUpdatingTopNStateContract(
        new RowDataSerializer(KEY), selector(0), selector(0, 2), selector(1), KEY, INPUT, top1);
  }

  private static org.apache.flink.table.runtime.keyselector.RowDataKeySelector selector(
      int... columns) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalUpdateRankStateInteropTest.class.getClassLoader(),
        columns,
        InternalTypeInfo.of(INPUT));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static TypeSerializer<Tuple2<RowData, Integer>> tupleSerializer() {
    return new TupleSerializer(
        (Class) Tuple2.class,
        new TypeSerializer<?>[] {new RowDataSerializer(INPUT), IntSerializer.INSTANCE});
  }

  private static MapState<RowData, Tuple2<RowData, Integer>> data(
      CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            "data-state-with-update", new RowDataSerializer(ROW_KEY), tupleSerializer()));
  }

  private long create(boolean persistent, boolean top1) throws Exception {
    return create(persistent, top1, 2000);
  }

  private long create(boolean persistent, boolean top1, long limit) throws Exception {
    return create(persistent, top1, limit, 1 << 24);
  }

  private long create(boolean persistent, boolean top1, long limit, long budget) throws Exception {
    return create(persistent, top1, limit, budget, 0, new byte[0][]);
  }

  private long create(
      boolean persistent,
      boolean top1,
      long limit,
      long budget,
      long restoredAt,
      byte[][] snapshots)
      throws Exception {
    int[] partition = {0};
    int[] precision = {Native.STOCK_KEY_PROJECTION, -1};
    int[] rowKeys = {0, 2};
    int[] rowPrecision = {Native.STOCK_KEY_PROJECTION, -1, -1};
    int[] sort = {1};
    int[] order = {1};
    if (!persistent && snapshots.length != 0)
      return Native.restoreUpdateFastTopNRankerPartitions(
          partition,
          precision,
          rowKeys,
          rowPrecision,
          sort,
          order,
          order,
          0,
          top1 ? 1 : limit,
          -1,
          true,
          true,
          0,
          restoredAt,
          snapshots,
          budget);
    if (!persistent)
      return Native.createUpdateFastTopNRanker(
          partition,
          precision,
          rowKeys,
          rowPrecision,
          sort,
          order,
          order,
          0,
          top1 ? 1 : limit,
          -1,
          true,
          true,
          0,
          budget);
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        AutoCloseable release =
            () -> {
              if (schema.snapshot().release != 0) schema.release();
            }) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(INPUT),
          NativeAllocator.DICTIONARIES,
          schema);
      return Native.createRocksDBUpdateFastTopNRanker(
          partition,
          precision,
          rowKeys,
          rowPrecision,
          sort,
          order,
          order,
          0,
          top1 ? 1 : limit,
          -1,
          true,
          true,
          0,
          restoredAt,
          budget,
          schema.memoryAddress(),
          directory.resolve("state-" + java.util.UUID.randomUUID()).toString(),
          128,
          FlinkRocksDBOptions.from(new Configuration()).json(),
          0,
          new String[0],
          new String[0],
          0,
          127,
          false,
          snapshots);
    }
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBTopNRanker(handle);
    else Native.closeTopNRanker(handle);
  }

  private static RowData key(long value) {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(value)).copy();
  }

  private static RowData rowKey(long value) {
    return new RowDataSerializer(ROW_KEY).toBinaryRow(GenericRowData.of(1L, value)).copy();
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness()
      throws Exception {
    return harness(false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock) throws Exception {
    return harness(stock, false, true, true);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock, boolean rocks, boolean before, boolean projected) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator = new Projection();
    if (stock) {
      var type = InternalTypeInfo.<RowData>of(INPUT);
      var comparator =
          ComparatorCodeGenerator.gen(
              new Configuration(),
              CanonicalUpdateRankStateInteropTest.class.getClassLoader(),
              "CanonicalUpdatingRankComparator",
              KEY,
              SortSpec.builder().addField(0, true, false).build());
      var function =
          new UpdatableTopNFunction(
              StateConfigUtil.createTtlConfig(0),
              type,
              KeySelectorUtil.getRowDataSelector(
                  CanonicalUpdateRankStateInteropTest.class.getClassLoader(),
                  new int[] {0, 2},
                  type),
              comparator,
              KeySelectorUtil.getRowDataSelector(
                  CanonicalUpdateRankStateInteropTest.class.getClassLoader(), new int[] {1}, type),
              RankType.ROW_NUMBER,
              new ConstantRankRange(1, 3),
              before,
              projected,
              10000);
      var process = new KeyedProcessOperator<RowData, RowData, RowData>(function);
      function.setKeyContext(process);
      operator = process;
    }
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalUpdateRankStateInteropTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(INPUT));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(projected ? OUTPUT : INPUT));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
