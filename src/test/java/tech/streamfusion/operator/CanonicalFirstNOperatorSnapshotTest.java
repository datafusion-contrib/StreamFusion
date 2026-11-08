package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.Native;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.CanonicalProjection;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class CanonicalFirstNOperatorSnapshotTest {
  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void canonicalLifecycleContinuesInBothEnginesWithoutMaterializingAProjection(
      boolean rocks, boolean checkpointFirst) throws Exception {
    try (var source = nativeHarness(rocks);
        var reference = harness(true, false, true, 0)) {
      source.open();
      reference.open();
      var prefix =
          List.<RowData>of(
              GenericRowData.of(1L, 10L), GenericRowData.of(2L, 20L), GenericRowData.of(1L, 30L));
      assertEquals(stockPush(reference, prefix), push(source, prefix));
      if (checkpointFirst) {
        var operator = (NativeOperator) source.getOperator();
        operator.nativeCheckpoint = true;
        try {
          source.snapshot(1, 0);
        } finally {
          operator.nativeCheckpoint = false;
        }
      }
      var exported =
          source
              .snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      var projectionField =
          AbstractNativeStatefulOperator.class.getDeclaredField("canonicalProjection");
      projectionField.setAccessible(true);
      var backendField = CanonicalProjection.class.getDeclaredField("backend");
      backendField.setAccessible(true);
      assertNull(backendField.get(projectionField.get(source.getOperator())));
      try (var stock = harness(true, false, true, 0)) {
        stock.initializeState(exported);
        stock.open();
        var suffix =
            List.<RowData>of(
                GenericRowData.of(1L, 40L), GenericRowData.of(1L, 50L), GenericRowData.of(2L, 60L));
        var expected = stockPush(reference, suffix);
        assertEquals(expected, stockPush(stock, suffix));
        assertEquals(expected, push(source, suffix));
        try (var resumed = nativeHarness(!rocks)) {
          resumed.initializeState(snapshot(stock));
          resumed.open();
          var more =
              List.<RowData>of(
                  GenericRowData.of(2L, 70L),
                  GenericRowData.of(3L, 80L),
                  GenericRowData.of(1L, 90L));
          expected = stockPush(reference, more);
          assertEquals(expected, push(resumed, more));
          assertEquals(expected, stockPush(stock, more));
        }
      }
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void backendDisposalWaitsForTheAsynchronousCanonicalReader(boolean rocks) throws Exception {
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    org.apache.flink.streaming.api.operators.OperatorSnapshotFutures futures = null;
    var source = nativeHarness(rocks);
    try {
      source.open();
      push(source, List.of(GenericRowData.of(1L, 10L)));
      var options =
          org.apache.flink.runtime.checkpoint.CheckpointOptions.forConfig(
              SavepointType.savepoint(SavepointFormatType.CANONICAL),
              org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault(),
              true,
              false,
              0);
      var operator = (NativeOperator) source.getOperator();
      futures =
          operator.snapshotState(
              9,
              0,
              options,
              new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20));
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
      var saved = tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(futures);
      closing.get(5, java.util.concurrent.TimeUnit.SECONDS);
      try (var stock = harness(true, false, true, 0)) {
        stock.initializeState(saved);
        stock.open();
        assertEquals(List.of("1|20|2"), stockPush(stock, List.of(GenericRowData.of(1L, 20L))));
      }
    } finally {
      try {
        if (futures != null) futures.cancel();
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

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rocks) throws Exception {
    var operator = new NativeOperator();
    var contract = contract(false, 0);
    operator.setStockStateContract(contract);
    int representative = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> representative, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          StateBackendOptions.STATE_BACKEND.key(),
          RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        StreamExecutionEnvironment.getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalFirstNOperatorSnapshotTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalFirstNOperatorSnapshotTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static List<String> push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      List<RowData> rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED))));
    var output = new ArrayList<String>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord<?>) {
        try (var batch = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(batch, outputType(true))) {
            assertEquals(org.apache.flink.types.RowKind.INSERT, row.getRowKind());
            output.add(row.getLong(0) + "|" + row.getLong(1) + "|" + row.getLong(2));
          }
        }
      }
    }
    return output;
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deferredAdaptersReadPinnedVersionsAndDoNoConversionAtCapture(boolean rocks)
      throws Exception {
    DeferredContract.exports.set(0);
    try (var source = nativeHarness(rocks)) {
      ((NativeOperator) source.getOperator()).setStockStateContract(new DeferredContract());
      source.open();
      push(source, List.of(GenericRowData.of(1L, 10L), GenericRowData.of(2L, 20L)));
      var first = capture(source, 10);
      org.apache.flink.streaming.api.operators.OperatorSnapshotFutures second = null;
      try {
        push(source, List.of(GenericRowData.of(1L, 30L), GenericRowData.of(2L, 40L)));
        second = capture(source, 11);
        push(source, List.of(GenericRowData.of(1L, 50L), GenericRowData.of(2L, 60L)));
        assertEquals(0, DeferredContract.exports.get(), "capture must not run the adapter");
        var later = tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(second);
        var earlier = tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(first);
        assertEquals(2, DeferredContract.exports.get());
        try (var before = harness(true, false, true, 0);
            var after = harness(true, false, true, 0)) {
          before.initializeState(earlier);
          after.initializeState(later);
          before.open();
          after.open();
          var suffix = List.<RowData>of(GenericRowData.of(1L, 70L), GenericRowData.of(2L, 80L));
          assertEquals(List.of("1|70|2", "2|80|2"), stockPush(before, suffix));
          assertEquals(List.of("1|70|3", "2|80|3"), stockPush(after, suffix));
        }
      } finally {
        first.cancel();
        if (second != null) second.cancel();
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void failedDeferredConversionReleasesItsReaderAndLeavesLiveStateRetryable(boolean rocks)
      throws Exception {
    try (var source = nativeHarness(rocks)) {
      var operator = (NativeOperator) source.getOperator();
      operator.setStockStateContract(new DeferredContract(true));
      source.open();
      push(source, List.of(GenericRowData.of(1L, 10L)));
      var failed = capture(source, 20);
      try {
        push(source, List.of(GenericRowData.of(1L, 20L)));
        var failure =
            assertThrows(
                Exception.class,
                () -> tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(failed));
        assertTrue(
            org.apache.flink.util.ExceptionUtils.stringifyException(failure)
                .contains("intentional deferred conversion failure"));
      } finally {
        failed.cancel();
      }
      operator.setStockStateContract(new DeferredContract());
      var retry = capture(source, 21);
      try {
        var saved = tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(retry);
        try (var stock = harness(true, false, true, 0)) {
          stock.initializeState(saved);
          stock.open();
          var suffix = List.<RowData>of(GenericRowData.of(1L, 30L));
          var expected = List.of("1|30|3");
          assertEquals(expected, stockPush(stock, suffix));
          assertEquals(expected, push(source, suffix));
        }
      } finally {
        retry.cancel();
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void cancellingDeferredCaptureBeforeWritingDoesNotRunTheAdapter(boolean rocks) throws Exception {
    DeferredContract.exports.set(0);
    try (var source = nativeHarness(rocks)) {
      ((NativeOperator) source.getOperator()).setStockStateContract(new DeferredContract());
      source.open();
      push(source, List.of(GenericRowData.of(1L, 10L)));
      var cancelled = capture(source, 30);
      cancelled.cancel();
      assertEquals(0, DeferredContract.exports.get());
      push(source, List.of(GenericRowData.of(1L, 20L)));
      var retry = capture(source, 31);
      try {
        var saved = tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(retry);
        assertEquals(1, DeferredContract.exports.get());
        try (var stock = harness(true, false, true, 0)) {
          stock.initializeState(saved);
          stock.open();
          assertEquals(List.of("1|30|3"), stockPush(stock, List.of(GenericRowData.of(1L, 30L))));
        }
      } finally {
        retry.cancel();
      }
    }
  }

  private static org.apache.flink.streaming.api.operators.OperatorSnapshotFutures capture(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> source, long id)
      throws Exception {
    source.prepareSnapshotPreBarrier(id);
    return source
        .getOperator()
        .snapshotState(
            id,
            0,
            org.apache.flink.runtime.checkpoint.CheckpointOptions.forConfig(
                SavepointType.savepoint(SavepointFormatType.CANONICAL),
                org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault(),
                true,
                false,
                0),
            new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(16 * 1024 * 1024));
  }

  private static final class DeferredContract
      implements tech.streamfusion.state.StockStateContract {
    private static final java.util.concurrent.atomic.AtomicInteger exports =
        new java.util.concurrent.atomic.AtomicInteger();
    private final tech.streamfusion.state.StockStateContract delegate = contract(false, 0);
    private final boolean failAfterExport;

    DeferredContract() {
      this(false);
    }

    DeferredContract(boolean failAfterExport) {
      this.failAfterExport = failAfterExport;
    }

    @Override
    public org.apache.flink.api.common.typeutils.TypeSerializer<RowData> keySerializer() {
      return delegate.keySerializer();
    }

    @Override
    public java.util.Map<String, org.apache.flink.api.common.state.StateDescriptor.Type>
        stateDescriptors() {
      return delegate.stateDescriptors();
    }

    @Override
    public void exportState(
        org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData> backend,
        long handle,
        boolean persistent)
        throws Exception {
      exports.incrementAndGet();
      delegate.exportState(backend, handle, persistent);
      if (failAfterExport) throw new java.io.IOException("intentional deferred conversion failure");
    }

    @Override
    public void importState(
        org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData> backend,
        long handle,
        boolean persistent)
        throws Exception {
      delegate.importState(backend, handle, persistent);
    }
  }

  private static final class NativeOperator extends NativeColumnarFirstNOperator {
    private boolean nativeCheckpoint;

    NativeOperator() {
      super(new int[] {0}, new int[] {Native.STOCK_KEY_PROJECTION, -1}, 3, true, 0, 128);
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      assertTrue(nativeCheckpoint, "Canonical export must not first write a native snapshot");
      return super.snapshotCanonicalPartitions();
    }
  }
}
