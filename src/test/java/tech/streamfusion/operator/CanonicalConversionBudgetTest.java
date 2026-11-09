package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;

class CanonicalConversionBudgetTest {
  @BeforeAll
  static void memory() {
    CanonicalTemporalSortInteropTest.memory();
  }

  @Test
  void refusedSortFrameReleasesItsScopeBeforeRetryAndContinuedInput() throws Exception {
    var plan = new CanonicalTemporalSortInteropTest.Plan("map", 128);
    try (var source = plan.nativeHarness(true)) {
      source.open();
      for (int index = 0; index < 3; index++) plan.push(source, plan.row(index, 100 + index));
      source.prepareSnapshotPreBarrier(0);
      var original = CanonicalTemporalSortInteropTest.snapshot(source);
      var digest = plan.digest(original);
      long handle = ((AbstractNativeStatefulOperator<?>) source.getOperator()).handle;
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      long reserved = TaskOffHeapMemory.reservedBytes();
      String handles = Native.liveNativeHandles();
      long scope = Native.createCanonicalMemoryContext(0);
      try {
        assertThrows(
            NativeMemoryLimitException.class,
            () ->
                Native.withCanonicalDatabases(
                    scope,
                    () -> {
                      Native.exportTemporalSortCanonicalRows(
                          handle,
                          (array, schema) -> {
                            fail("A denied scan must not export Arrow buffers");
                          });
                      return 0;
                    }));
      } finally {
        Native.closeCanonicalDatabases(scope);
      }
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
      assertEquals(handles, Native.liveNativeHandles());
      assertEquals(digest, plan.digest(CanonicalTemporalSortInteropTest.snapshot(source)));
      try (var stock = plan.stock(false)) {
        stock.initializeState(original);
        stock.open();
        var row = plan.row(4, 150);
        plan.push(source, row);
        plan.pushStock(stock, row);
        source.processWatermark(new Watermark(1000));
        stock.processWatermark(new Watermark(1000));
        var expected = plan.stockOutput(stock);
        assertFalse(expected.isEmpty());
        assertEquals(expected, plan.nativeOutput(source));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void refusedWindowRankFrameLeavesStateAvailableForRetryAndNewInput(boolean dedup)
      throws Exception {
    var plan =
        new CanonicalWindowRankInteropTest.StockPlan("TUMBLE", "UTC", false, false, false, dedup);
    try (var source = plan.nativeHarness(true)) {
      source.open();
      long end = CanonicalWindowRankInteropTest.BASE + 3000;
      for (int index = 0; index < 3; index++)
        plan.sendNative(source, plan.row(1L, (long) index, index, end));
      source.prepareSnapshotPreBarrier(0);
      var original = CanonicalTemporalSortInteropTest.snapshot(source);
      long handle = ((AbstractNativeStatefulOperator<?>) source.getOperator()).handle;
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      long reserved = TaskOffHeapMemory.reservedBytes();
      String handles = Native.liveNativeHandles();
      long scope = Native.createCanonicalMemoryContext(0);
      try {
        assertThrows(
            NativeMemoryLimitException.class,
            () ->
                Native.withCanonicalDatabases(
                    scope,
                    () -> {
                      Native.exportWindowRankerCanonical(
                          handle,
                          (array, schema) -> {
                            fail("A denied bucket must not export Arrow buffers");
                          });
                      return 0;
                    }));
      } finally {
        Native.closeCanonicalDatabases(scope);
      }
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
      assertEquals(handles, Native.liveNativeHandles());
      try (var stock = plan.stock(false)) {
        stock.initializeState(original);
        stock.open();
        var row = plan.row(1L, -10L, 99, end);
        plan.sendNative(source, row);
        plan.send(stock, row);
        source.processWatermark(Watermark.MAX_WATERMARK);
        stock.processWatermark(Watermark.MAX_WATERMARK);
        var expected = plan.output(stock);
        assertFalse(expected.isEmpty());
        assertEquals(expected, plan.nativeOutput(source));
      }
    }
  }

  @Test
  void deferredWriterKeepsItsMemoryOwnerWhileTheLiveOperatorCloses() throws Exception {
    var plan = new CanonicalTemporalSortInteropTest.Plan("map", 128);
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    org.apache.flink.streaming.api.operators.OperatorSnapshotFutures captured = null;
    var source = plan.nativeHarness(true);
    try {
      source.open();
      for (int index = 0; index < 3; index++) plan.push(source, plan.row(index, 100 + index));
      captured = CanonicalDistinctWindowAsyncTest.capture(source, 10);
      var entered = new java.util.concurrent.CountDownLatch(1);
      var closing =
          executor.submit(
              () -> {
                entered.countDown();
                source.getOperator().close();
                return null;
              });
      assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
      assertThrows(
          java.util.concurrent.TimeoutException.class,
          () -> closing.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
      var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured);
      closing.get(5, java.util.concurrent.TimeUnit.SECONDS);
      try (var stock = plan.stock(false)) {
        stock.initializeState(saved);
        stock.open();
        stock.processWatermark(new Watermark(1000));
        assertEquals(3, plan.stockOutput(stock).size());
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
}
