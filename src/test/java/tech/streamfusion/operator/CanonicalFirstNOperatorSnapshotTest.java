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

  @org.junit.jupiter.api.Test
  void backendDisposalWaitsForTheAsynchronousCanonicalReader() throws Exception {
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    org.apache.flink.streaming.api.operators.OperatorSnapshotFutures futures = null;
    var source = nativeHarness(true);
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
      var saved =
          new org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer(futures)
              .getJobManagerOwnedState();
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
