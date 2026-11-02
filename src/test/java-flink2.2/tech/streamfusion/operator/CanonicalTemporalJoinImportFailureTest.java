package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.StockTemporalJoinStateContract;

class CanonicalTemporalJoinImportFailureTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (String fault :
        List.of(
            "foreign",
            "registered",
            "missing-event",
            "cleanup",
            "unowned-processing",
            "row-key",
            "build-time",
            "cleanup-disabled"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          result.add(Arguments.of(fault, stock, nativeStore));
    return result.stream();
  }

  @ParameterizedTest(name = "{0} stock={1} native={2}")
  @MethodSource("cases")
  void malformedStockStateFailsBeforeAnyNativeMutation(
      String fault, boolean stockStore, boolean nativeStore) throws Exception {
    long retention = fault.equals("cleanup-disabled") ? 0 : 100;
    try (var source = CanonicalTemporalMetadataFixtureTest.harness(stockStore, retention);
        var target = CanonicalTemporalJoinInteropTest.nativeHarness(nativeStore, retention, true)) {
      source.open();
      target.open();
      source.processElement1(new StreamRecord<>(CanonicalTemporalMetadataFixtureTest.data(1, 100)));
      source.processElement2(
          new StreamRecord<>(CanonicalTemporalMetadataFixtureTest.data(10, 100)));
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              source.getOperator().<RowData>getKeyedStateBackend();
      var key = new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(1L)).copy();
      backend.setCurrentKey(key);
      String reason;
      switch (fault) {
        case "foreign":
          update(backend, "foreign", 1);
          reason = "not owned";
          break;
        case "cleanup-disabled":
          update(backend, "cleanup-timestamp", 150);
          reason = "not owned";
          break;
        case "registered":
          update(backend, "timer", 99);
          reason = "Registered temporal value";
          break;
        case "cleanup":
          update(backend, "cleanup-timestamp", 151);
          reason = "cleanup value";
          break;
        case "missing-event":
          var eventOperator =
              (org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperator)
                  source.getOperator();
          eventOperator
              .getInternalTimerService("timers", VoidNamespaceSerializer.INSTANCE, eventOperator)
              .deleteEventTimeTimer(VoidNamespace.INSTANCE, 100);
          reason = "Registered temporal value";
          break;
        case "unowned-processing":
          var processingOperator =
              (org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperator)
                  source.getOperator();
          processingOperator
              .getInternalTimerService(
                  "timers", VoidNamespaceSerializer.INSTANCE, processingOperator)
              .registerProcessingTimeTimer(VoidNamespace.INSTANCE, 300);
          reason = "cleanup value";
          break;
        case "row-key":
          backend
              .getPartitionedState(
                  VoidNamespace.INSTANCE,
                  VoidNamespaceSerializer.INSTANCE,
                  new MapStateDescriptor<>(
                      "left", LongSerializer.INSTANCE, new RowDataSerializer(INPUT)))
              .put(
                  0L,
                  new RowDataSerializer(INPUT).toBinaryRow(GenericRowData.of(2L, 1L, 100L)).copy());
          reason = "Temporal row disagrees";
          break;
        case "build-time":
          backend
              .getPartitionedState(
                  VoidNamespace.INSTANCE,
                  VoidNamespaceSerializer.INSTANCE,
                  new MapStateDescriptor<>(
                      "right", LongSerializer.INSTANCE, new RowDataSerializer(INPUT)))
              .put(99L, CanonicalTemporalMetadataFixtureTest.data(10, 100));
          reason = "Temporal row disagrees";
          break;
        default:
          throw new AssertionError(fault);
      }
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(
                  1,
                  0,
                  org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                      org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = CanonicalTemporalMetadataFixtureTest.harness(stockStore, retention)) {
        restored.initializeState(saved);
        var restoredBackend =
            (CheckpointableKeyedStateBackend<RowData>)
                restored.getOperator().<RowData>getKeyedStateBackend();
        var contract =
            new StockTemporalJoinStateContract(
                new RowDataSerializer(KEY),
                KEY,
                INPUT,
                INPUT,
                new int[] {0},
                new int[] {0},
                2,
                2,
                retention);
        var operator = (NativeTemporalJoinOperator) target.getOperator();
        var failure =
            assertThrows(
                IllegalStateException.class,
                () -> contract.importState(restoredBackend, operator.handle, nativeStore));
        assertTrue(failure.getMessage().contains(reason), failure.toString());
        var rows = new AtomicInteger();
        Native.BatchReceiver inspect =
            (a, s) -> {
              try (var array = ArrowArray.wrap(a);
                  var schema = ArrowSchema.wrap(s);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
                rows.addAndGet(root.getRowCount());
              }
            };
        Native.exportTemporalJoinCanonicalRows(operator.handle, true, inspect);
        Native.exportTemporalJoinCanonicalRows(operator.handle, false, inspect);
        Native.exportTemporalJoinCanonicalMetadata(operator.handle, inspect);
        assertEquals(0, rows.get(), "Preflight failure partially imported native state");
        assertArrayEquals(new long[0], Native.nextTemporalJoinCleanup(operator.handle));
      } finally {
        saved.discardState();
      }
    }
  }

  private static void update(
      CheckpointableKeyedStateBackend<RowData> backend, String name, long value) throws Exception {
    backend
        .getPartitionedState(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            new ValueStateDescriptor<>(name, LongSerializer.INSTANCE))
        .update(value);
  }
}
