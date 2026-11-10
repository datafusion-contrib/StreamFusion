package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class StockCanonicalSnapshotTest {
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final ListStateDescriptor<Long> WATERMARK =
      new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE);
  private static final ValueStateDescriptor<Long> ACCUMULATOR =
      new ValueStateDescriptor<>("window-aggs", LongSerializer.INSTANCE);
  private static final CheckpointOptions OPTIONS =
      new CheckpointOptions(
          SavepointType.savepoint(SavepointFormatType.CANONICAL),
          CheckpointStorageLocationReference.getDefault());

  @ParameterizedTest
  @ValueSource(strings = {"complete", "cancel", "task-cancel", "closed-registry", "failed-keyed"})
  void operatorStateRetainsItsBackendAfterKeyedStateFinishes(String mode) throws Exception {
    var source = harness(true);
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    OperatorSnapshotFutures captured = null;
    try {
      source.open();
      var operator = (Fixture) source.getOperator();
      var operatorBackend = operator.getOperatorStateBackend();
      assertInstanceOf(CanonicalOperatorStateBackend.class, operatorBackend);
      operator.store.getUnionListState(WATERMARK).update(List.of(123L));
      if (mode.equals("closed-registry")) {
        operator.cancelables().close();
        assertThrows(
            IOException.class,
            () -> operatorBackend.snapshot(1, 0, new MemCheckpointStreamFactory(1 << 20), OPTIONS));
      } else {
        boolean failedKeyed = mode.equals("failed-keyed");
        if (failedKeyed) {
          var keyed = backend(source);
          keyed.setCurrentKey(key());
          keyed.getPartitionedState(1000L, LongSerializer.INSTANCE, ACCUMULATOR).update(7L);
        }
        var streams =
            new MemCheckpointStreamFactory(1 << 20) {
              @Override
              public org.apache.flink.runtime.state.CheckpointStateOutputStream
                  createCheckpointStateOutputStream(
                      org.apache.flink.runtime.state.CheckpointedStateScope scope)
                      throws IOException {
                if (failedKeyed) throw new IOException("failed keyed write");
                return super.createCheckpointStateOutputStream(scope);
              }
            };
        captured = operator.snapshotState(1, 0, OPTIONS, streams);
        if (failedKeyed) {
          var failed = captured;
          Exception failure =
              assertThrows(
                  Exception.class,
                  () -> tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(failed));
          Throwable cause = failure;
          while (cause.getCause() != null) cause = cause.getCause();
          assertEquals("failed keyed write", cause.getMessage());
          assertFalse(captured.getOperatorStateManagedFuture().isDone());
        } else {
          org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(
              captured.getKeyedStateManagedFuture());
        }
        var entered = new java.util.concurrent.CountDownLatch(1);
        var closing =
            executor.submit(
                () -> {
                  entered.countDown();
                  operator.close();
                  return null;
                });
        assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
        assertThrows(
            java.util.concurrent.TimeoutException.class,
            () -> closing.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
        if (mode.equals("cancel") || failedKeyed) captured.cancel();
        else if (mode.equals("task-cancel")) operator.cancelables().close();
        else {
          var saved = finish(captured);
          try (var restored = harness()) {
            restored.initializeState(saved);
            restored.open();
            assertEquals(
                List.of(123L),
                values(
                    ((Fixture) restored.getOperator()).store.getUnionListState(WATERMARK).get()));
          }
        }
        closing.get(5, java.util.concurrent.TimeUnit.SECONDS);
      }
    } finally {
      try {
        if (captured != null) captured.cancel();
      } finally {
        executor.shutdownNow();
        try {
          assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
          source.close();
        }
      }
    }
  }

  @Test
  void keyedAccumulatorsAndUnionWatermarkBelongToTheSameImmutableCapture() throws Exception {
    try (var source = harness();
        var cancelables = new CloseableRegistry()) {
      source.open();
      var backend = backend(source);
      backend.setCurrentKey(key());
      backend.getPartitionedState(1000L, LongSerializer.INSTANCE, ACCUMULATOR).update(7L);
      backend.getPartitionedState(3000L, LongSerializer.INSTANCE, ACCUMULATOR).update(11L);
      var operator = (Fixture) source.getOperator();
      operator.store.getUnionListState(WATERMARK).update(List.of(400L, 200L));
      var captured =
          StockCanonicalSnapshot.capture(
              backend,
              operator.getOperatorStateBackend(),
              1,
              0,
              new MemCheckpointStreamFactory(1 << 20),
              OPTIONS,
              cancelables);
      backend.getPartitionedState(1000L, LongSerializer.INSTANCE, ACCUMULATOR).clear();
      backend.getPartitionedState(3000L, LongSerializer.INSTANCE, ACCUMULATOR).update(99L);
      operator.store.getUnionListState(WATERMARK).update(List.of(900L));
      var saved = finish(captured);
      assertEquals(1, saved.getManagedOperatorState().size());
      var metadata =
          saved.getManagedOperatorState().iterator().next().getStateNameToPartitionOffsets();
      assertEquals(OperatorStateHandle.Mode.UNION, metadata.get("watermark").getDistributionMode());
      assertTrue(saved.getRawKeyedState().isEmpty());
      assertTrue(saved.getRawOperatorState().isEmpty());
      try (var restored = harness()) {
        restored.initializeState(saved);
        restored.open();
        var actual = backend(restored);
        actual.setCurrentKey(key());
        assertEquals(
            7L, actual.getPartitionedState(1000L, LongSerializer.INSTANCE, ACCUMULATOR).value());
        assertEquals(
            11L, actual.getPartitionedState(3000L, LongSerializer.INSTANCE, ACCUMULATOR).value());
        var target = (Fixture) restored.getOperator();
        var watermark = new StockWindowWatermark(target.store, true);
        assertEquals(200L, watermark.restoredWatermark());
        assertEquals(List.of(400L, 200L), values(target.store.getUnionListState(WATERMARK).get()));
        watermark.snapshot(500);
        assertEquals(List.of(500L), values(target.store.getUnionListState(WATERMARK).get()));
      }
    }
  }

  static Stream<Arguments> watermarks() {
    return Stream.of(
        Arguments.of(List.of(), Long.MIN_VALUE),
        Arguments.of(List.of(400L, 200L), 200L),
        Arguments.of(List.of(Long.MIN_VALUE, 200L), Long.MIN_VALUE),
        Arguments.of(List.of(Long.MAX_VALUE), Long.MIN_VALUE),
        Arguments.of(List.of(Long.MAX_VALUE, 200L), 200L),
        Arguments.of(List.of(-500L, -1000L), -1000L));
  }

  @ParameterizedTest
  @MethodSource("watermarks")
  void unionRestorationUsesTheReleasedMinimumAndSentinelRule(List<Long> values, long expected)
      throws Exception {
    try (var source = harness();
        var cancelables = new CloseableRegistry()) {
      source.open();
      var operator = (Fixture) source.getOperator();
      operator.store.getUnionListState(WATERMARK).update(values);
      var snapshot =
          finish(
              StockCanonicalSnapshot.capture(
                  backend(source),
                  operator.getOperatorStateBackend(),
                  1,
                  0,
                  new MemCheckpointStreamFactory(1 << 20),
                  OPTIONS,
                  cancelables));
      try (var restored = harness()) {
        restored.initializeState(snapshot);
        restored.open();
        var watermark = new StockWindowWatermark(((Fixture) restored.getOperator()).store, true);
        assertEquals(expected, watermark.restoredWatermark());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void unknownOperatorAndBroadcastStateCannotBeSilentlyIgnored(boolean broadcast, boolean restored)
      throws Exception {
    for (boolean populated : new boolean[] {false, true}) {
      try (var view = harness();
          var cancelables = new CloseableRegistry()) {
        view.open();
        var store = ((Fixture) view.getOperator()).store;
        if (broadcast) {
          var state =
              store.getBroadcastState(
                  new MapStateDescriptor<>(
                      "unexpected", IntSerializer.INSTANCE, LongSerializer.INSTANCE));
          if (populated) state.put(1, 7L);
        } else {
          var state =
              store.getListState(new ListStateDescriptor<>("unexpected", LongSerializer.INSTANCE));
          if (populated) state.update(List.of(7L));
        }
        if (restored) {
          try (var target = harness()) {
            target.initializeState(
                finish(
                    StockCanonicalSnapshot.capture(
                        backend(view),
                        view.getOperator().getOperatorStateBackend(),
                        1,
                        0,
                        new MemCheckpointStreamFactory(1 << 20),
                        OPTIONS,
                        cancelables)));
            target.open();
            assertThrows(
                IllegalStateException.class,
                () -> new StockWindowWatermark(((Fixture) target.getOperator()).store, true));
          }
        } else
          assertThrows(IllegalStateException.class, () -> new StockWindowWatermark(store, false));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void wrongWatermarkDistributionModeIsRejected(boolean restored) throws Exception {
    try (var view = harness();
        var cancelables = new CloseableRegistry()) {
      view.open();
      var store = ((Fixture) view.getOperator()).store;
      store.getListState(WATERMARK).update(List.of(200L));
      if (restored) {
        try (var target = harness()) {
          target.initializeState(
              finish(
                  StockCanonicalSnapshot.capture(
                      backend(view),
                      view.getOperator().getOperatorStateBackend(),
                      1,
                      0,
                      new MemCheckpointStreamFactory(1 << 20),
                      OPTIONS,
                      cancelables)));
          target.open();
          assertThrows(
              IllegalStateException.class,
              () -> new StockWindowWatermark(((Fixture) target.getOperator()).store, true));
        }
      } else
        assertThrows(IllegalStateException.class, () -> new StockWindowWatermark(store, false));
    }
  }

  @Test
  void restoredWatermarkRequiresTheReleasedLongSerializer() throws Exception {
    try (var view = harness();
        var cancelables = new CloseableRegistry()) {
      view.open();
      var operator = (Fixture) view.getOperator();
      operator
          .store
          .getUnionListState(new ListStateDescriptor<>("watermark", IntSerializer.INSTANCE))
          .update(List.of(200));
      var snapshot =
          finish(
              StockCanonicalSnapshot.capture(
                  backend(view),
                  operator.getOperatorStateBackend(),
                  1,
                  0,
                  new MemCheckpointStreamFactory(1 << 20),
                  OPTIONS,
                  cancelables));
      try (var restored = harness()) {
        restored.initializeState(snapshot);
        restored.open();
        assertThrows(
            org.apache.flink.util.StateMigrationException.class,
            () -> new StockWindowWatermark(((Fixture) restored.getOperator()).store, true));
      }
    }
  }

  @Test
  void cancellationReleasesBothCapturesAndLeavesLiveStateUsableForRetry() throws Exception {
    try (var view = harness();
        var cancelables = new CloseableRegistry()) {
      view.open();
      var operator = (Fixture) view.getOperator();
      operator.store.getUnionListState(WATERMARK).update(List.of(200L));
      var state = backend(view);
      state.setCurrentKey(key());
      state.getPartitionedState(1000L, LongSerializer.INSTANCE, ACCUMULATOR).update(7L);
      var captured =
          StockCanonicalSnapshot.capture(
              state,
              operator.getOperatorStateBackend(),
              1,
              0,
              new MemCheckpointStreamFactory(1 << 20),
              OPTIONS,
              cancelables);
      captured.cancel();
      assertTrue(captured.getOperatorStateManagedFuture().isCancelled());
      assertEquals(
          7L, state.getPartitionedState(1000L, LongSerializer.INSTANCE, ACCUMULATOR).value());
      var retried =
          finish(
              StockCanonicalSnapshot.capture(
                  state,
                  operator.getOperatorStateBackend(),
                  2,
                  0,
                  new MemCheckpointStreamFactory(1 << 20),
                  OPTIONS,
                  cancelables));
      assertFalse(retried.getManagedKeyedState().isEmpty());
      assertFalse(retried.getManagedOperatorState().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void keyedCaptureFailureCancelsOperatorCaptureAndPreservesTheOriginalError(boolean failCancel)
      throws Exception {
    var failure = new IOException("keyed capture failed");
    var future =
        new FutureTask<SnapshotResult<OperatorStateHandle>>(SnapshotResult::empty) {
          @Override
          public boolean cancel(boolean interrupt) {
            if (failCancel) throw new IllegalStateException("cancel failed");
            return super.cancel(interrupt);
          }
        };
    var keyed =
        (CheckpointableKeyedStateBackend<RowData>)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {CheckpointableKeyedStateBackend.class},
                (proxy, method, arguments) -> {
                  throw failure;
                });
    var operator =
        (OperatorStateBackend)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {OperatorStateBackend.class},
                (proxy, method, arguments) -> {
                  assertEquals("snapshot", method.getName());
                  return future;
                });
    try (var cancelables = new CloseableRegistry()) {
      assertSame(
          failure,
          assertThrows(
              IOException.class,
              () ->
                  StockCanonicalSnapshot.capture(
                      keyed,
                      operator,
                      1,
                      0,
                      new MemCheckpointStreamFactory(1 << 20),
                      OPTIONS,
                      cancelables)));
      if (failCancel) assertEquals(1, failure.getSuppressed().length);
      else assertTrue(future.isCancelled());
    }
  }

  @Test
  void operatorCaptureFailureDoesNotAllocateKeyedResources() throws Exception {
    var probe = new StockSnapshotProbe(List.of());
    var failure = new IOException("operator capture failed");
    var operator =
        (OperatorStateBackend)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {OperatorStateBackend.class},
                (proxy, method, arguments) -> {
                  throw failure;
                });
    try (var cancelables = new CloseableRegistry()) {
      assertSame(
          failure,
          assertThrows(
              IOException.class,
              () ->
                  StockCanonicalSnapshot.capture(
                      probe.backend,
                      operator,
                      1,
                      0,
                      new MemCheckpointStreamFactory(1 << 20),
                      OPTIONS,
                      cancelables)));
      assertEquals(0, probe.releases);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nativeFormatsNeverEnterStockCanonicalCapture(boolean nativeSavepoint) throws Exception {
    var probe = new StockSnapshotProbe(List.of());
    var options =
        new CheckpointOptions(
            nativeSavepoint
                ? SavepointType.savepoint(SavepointFormatType.NATIVE)
                : CheckpointType.CHECKPOINT,
            CheckpointStorageLocationReference.getDefault());
    try (var cancelables = new CloseableRegistry()) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              StockCanonicalSnapshot.capture(
                  probe.backend,
                  null,
                  1,
                  0,
                  new MemCheckpointStreamFactory(1 << 20),
                  options,
                  cancelables));
      assertEquals(0, probe.releases);
    }
  }

  private static List<Long> values(Iterable<Long> input) {
    var values = new ArrayList<Long>();
    input.forEach(values::add);
    return values;
  }

  private static OperatorSubtaskState finish(OperatorSnapshotFutures futures) throws Exception {
    futures.getKeyedStateManagedFuture().run();
    futures.getOperatorStateManagedFuture().run();
    var keyed = futures.getKeyedStateManagedFuture().get().getJobManagerOwnedSnapshot();
    var operator = futures.getOperatorStateManagedFuture().get().getJobManagerOwnedSnapshot();
    var result = OperatorSubtaskState.builder();
    if (keyed != null) result.setManagedKeyedState(new StateObjectCollection<>(List.of(keyed)));
    if (operator != null)
      result.setManagedOperatorState(new StateObjectCollection<>(List.of(operator)));
    return result.build();
  }

  private static RowData key() {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(1L)).copy();
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness()
      throws Exception {
    return harness(false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean canonical) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            StockCanonicalSnapshotTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(KEY));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new Fixture(), selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        canonical
            ? new CanonicalStateBackend(
                new HashMapStateBackend(),
                java.util.Map.of(
                    harness.getStreamConfig().getOperatorID().toHexString(),
                    new CanonicalStateBackend.BoundContract(
                        Fixture.class.getSimpleName(),
                        new StockFirstNStateContract(new RowDataSerializer(KEY), KEY, false))))
            : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(KEY));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view) {
    return (CheckpointableKeyedStateBackend<RowData>)
        view.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Fixture extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    transient OperatorStateStore store;

    CloseableRegistry cancelables() {
      return getContainingTask().getCancelables();
    }

    @Override
    public void initializeState(StateInitializationContext context) {
      store = context.getOperatorStateStore();
      if (getKeyedStateBackend() instanceof CanonicalKeyedStateBackend) {
        CanonicalStateBackend.configureOperatorSnapshots(
            getOperatorStateBackend(),
            cancelables(),
            ((CanonicalKeyedStateBackend<?>) getKeyedStateBackend())::retainCanonicalReader);
      }
    }

    @Override
    public void close() throws Exception {
      if (getKeyedStateBackend() instanceof CanonicalKeyedStateBackend)
        ((CanonicalKeyedStateBackend<?>) getKeyedStateBackend()).closeCanonicalReaders();
      super.close();
    }

    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
