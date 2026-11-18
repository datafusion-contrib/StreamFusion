package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;

class CanonicalFirstNInteropTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "false,false,false", "false,false,true", "false,true,false", "false,true,true",
    "true,false,false", "true,false,true", "true,true,false", "true,true,true"
  })
  void stockCountersContinueThroughNativeAndStockCanonicalState(
      boolean rocks, boolean keyless, boolean rank) throws Exception {
    long handle = create(directory, rocks, keyless, rank, 0);
    try (var reference = harness(true, keyless, rank, 0);
        var view = harness(false, keyless, rank, 0)) {
      reference.open();
      stockPush(reference, List.of(GenericRowData.of(1L, 10L), GenericRowData.of(null, 20L)));
      view.initializeState(snapshot(reference));
      view.open();
      var contract = contract(keyless, 0);
      contract.importState(backend(view), handle, rocks);
      var suffix =
          List.<RowData>of(
              GenericRowData.of(1L, 30L),
              GenericRowData.of(2L, 40L),
              GenericRowData.of(1L, 50L),
              GenericRowData.of(null, 60L),
              GenericRowData.of(2L, 70L),
              GenericRowData.of(null, 80L));
      var expected = stockPush(reference, suffix);
      assertEquals(expected, nativePush(handle, suffix, rank, 0));
      assertFalse(expected.isEmpty());
      contract.exportState(backend(view), handle, rocks);
      try (var restored = harness(true, keyless, rank, 0)) {
        restored.initializeState(snapshot(view));
        restored.open();
        var more =
            List.<RowData>of(
                GenericRowData.of(2L, 90L),
                GenericRowData.of(3L, 100L),
                GenericRowData.of(null, 110L));
        expected = stockPush(reference, more);
        assertEquals(expected, stockPush(restored, more));
        assertEquals(expected, nativePush(handle, more, rank, 0));
      }
    } finally {
      Native.closeFirstN(handle);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void malformedCounterAfterTheFirstTransferChunkLeavesTheNativeTargetEmpty(
      boolean rocks, boolean wrongKeyKind) throws Exception {
    long handle = create(directory, rocks, false, true, 0);
    try (var view = harness(false, false, true, 0)) {
      view.open();
      var backend = backend(view);
      var values =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("counterState", IntSerializer.INSTANCE));
      var keys = new RowDataSerializer(keyType(false));
      for (long index = 0; index < 1026; index++) {
        var key = keys.toBinaryRow(GenericRowData.of(index)).copy();
        if (index == 1025 && wrongKeyKind) key.setRowKind(RowKind.DELETE);
        backend.setCurrentKey(key);
        values.update(index == 1025 && !wrongKeyKind ? -1 : 1);
      }
      var contract = contract(false, 0);
      assertThrows(
          RuntimeException.class, () -> contract.importState(ordered(backend), handle, rocks));
      contract.exportState(backend, handle, rocks);
      try (var saved = backend.getKeys("counterState", VoidNamespace.INSTANCE)) {
        assertEquals(0, saved.count(), "No validated earlier chunk may have been inserted");
      }
    } finally {
      Native.closeFirstN(handle);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void countersLargerThanOneTransferChunkSurviveExportAndContinuedInput(boolean rocks)
      throws Exception {
    long handle = create(directory, rocks, false, false, 0);
    try (var view = harness(false, false, false, 0)) {
      view.open();
      var backend = backend(view);
      var values =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("counterState", IntSerializer.INSTANCE));
      var keys = new RowDataSerializer(keyType(false));
      for (long index = 0; index < 2049; index++) {
        backend.setCurrentKey(keys.toBinaryRow(GenericRowData.of(index)).copy());
        values.update(2);
      }
      var contract = contract(false, 0);
      contract.importState(backend, handle, rocks);
      var chunks = new AtomicInteger();
      var rows = new AtomicInteger();
      Native.exportFirstNCanonical(
          handle,
          (array, schema) -> {
            try (var a = ArrowArray.wrap(array);
                var s = ArrowSchema.wrap(schema);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
              assertTrue(root.getRowCount() <= 1024);
              chunks.incrementAndGet();
              rows.addAndGet(root.getRowCount());
            }
          });
      assertEquals(3, chunks.get());
      assertEquals(2049, rows.get());
      contract.exportState(backend, handle, rocks);
      try (var restored = harness(true, false, false, 0)) {
        restored.initializeState(snapshot(view));
        restored.open();
        var input =
            List.<RowData>of(
                GenericRowData.of(0L, 1L),
                GenericRowData.of(1024L, 2L),
                GenericRowData.of(2048L, 3L),
                GenericRowData.of(0L, 4L));
        assertEquals(List.of("0|1", "1024|2", "2048|3"), nativePush(handle, input, false, 0));
        assertEquals(List.of("0|1", "1024|2", "2048|3"), stockPush(restored, input));
      }
    } finally {
      Native.closeFirstN(handle);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void existingTargetCounterInALaterChunkRejectsTheWholeView(boolean rocks) throws Exception {
    long handle = create(directory, rocks, false, true, 0);
    try (var view = harness(false, false, true, 0)) {
      view.open();
      nativePush(handle, List.of(GenericRowData.of(1025L, 1L)), true, 0);
      var backend = backend(view);
      var values =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("counterState", IntSerializer.INSTANCE));
      var keys = new RowDataSerializer(keyType(false));
      for (long index = 0; index < 1026; index++) {
        backend.setCurrentKey(keys.toBinaryRow(GenericRowData.of(index)).copy());
        values.update(1);
      }
      var contract = contract(false, 0);
      assertThrows(
          RuntimeException.class, () -> contract.importState(ordered(backend), handle, rocks));
      contract.exportState(backend, handle, rocks);
      try (var saved = backend.getKeys("counterState", VoidNamespace.INSTANCE)) {
        assertEquals(
            1, saved.count(), "An existing late target key must reject earlier fresh chunks");
      }
      assertEquals(
          List.of("1025|2|2"), nativePush(handle, List.of(GenericRowData.of(1025L, 2L)), true, 0));
    } finally {
      Native.closeFirstN(handle);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receiverFailureLeavesNativeCountersAvailableForRetry(boolean rocks) throws Exception {
    long handle = create(directory, rocks, false, true, 0);
    try (var view = harness(false, false, true, 0)) {
      view.open();
      nativePush(handle, List.of(GenericRowData.of(1L, 10L)), true, 0);
      assertThrows(
          RuntimeException.class,
          () ->
              Native.exportFirstNCanonical(
                  handle,
                  (array, schema) -> {
                    try (var a = ArrowArray.wrap(array);
                        var s = ArrowSchema.wrap(schema);
                        var root =
                            Data.importVectorSchemaRoot(
                                NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
                      assertEquals(1, root.getRowCount());
                      throw new IllegalStateException("first-N receiver failed");
                    }
                  }));
      contract(false, 0).exportState(backend(view), handle, rocks);
      try (var restored = harness(true, false, true, 0)) {
        restored.initializeState(snapshot(view));
        restored.open();
        var next = List.<RowData>of(GenericRowData.of(1L, 20L));
        assertEquals(List.of("1|20|2"), stockPush(restored, next));
        assertEquals(List.of("1|20|2"), nativePush(handle, next, true, 0));
      }
    } finally {
      Native.closeFirstN(handle);
    }
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> ordered(
      CheckpointableKeyedStateBackend<RowData> backend) {
    return (CheckpointableKeyedStateBackend<RowData>)
        Proxy.newProxyInstance(
            CanonicalFirstNInteropTest.class.getClassLoader(),
            new Class<?>[] {CheckpointableKeyedStateBackend.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getKeys"))
                return backend
                    .getKeys((String) args[0], args[1])
                    .sorted(java.util.Comparator.comparingLong(key -> key.getLong(0)));
              try {
                return method.invoke(backend, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
            });
  }
}
