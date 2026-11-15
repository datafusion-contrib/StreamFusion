package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class NativeResourcesTest {
  @Test
  void cleanupAttemptsEveryOwnerInReverseAcquisitionOrderAndIsIdempotent() throws Exception {
    var calls = new ArrayList<String>();
    var scope = new NativeResources();
    var handleFailure = new IllegalStateException("handle close");
    var bindingFailure = new IllegalArgumentException("binding close");
    scope.own(() -> calls.add("budget"));
    scope.own(
        () -> {
          calls.add("binding");
          throw bindingFailure;
        });
    scope.own(
        () -> {
          calls.add("handle");
          throw handleFailure;
        });

    assertSame(handleFailure, assertThrows(Exception.class, scope::close));
    assertEquals(List.of("handle", "binding", "budget"), calls);
    assertEquals(1, handleFailure.getSuppressed().length);
    assertSame(bindingFailure, handleFailure.getSuppressed()[0]);
    scope.close();
    assertEquals(3, calls.size());
  }

  @Test
  void registeringAfterCloseReleasesTheNewResource() throws Exception {
    var scope = new NativeResources();
    var calls = new ArrayList<String>();
    scope.close();
    assertThrows(Exception.class, () -> scope.own(() -> calls.add("late")));
    assertEquals(List.of("late"), calls);
  }

  @Test
  void fatalCleanupFailureDoesNotSkipTheRemainingResources() {
    var calls = new ArrayList<String>();
    var fatal = new AssertionError("native cleanup failed");
    Exception failure =
        assertThrows(
            Exception.class,
            () ->
                NativeResources.closeAll(
                    () -> {
                      throw fatal;
                    },
                    () -> calls.add("budget"),
                    () -> calls.add("host")));
    assertSame(fatal, failure.getCause());
    assertEquals(List.of("budget", "host"), calls);
  }

  @Test
  void failedStateConfigurationStillReleasesTheHandleBindingAndBudget() throws Exception {
    long previousCapacity = TaskOffHeapMemory.capacityBytes();
    TaskOffHeapMemory.resetForTests(1 << 20);
    var calls = new ArrayList<String>();
    var operator = new FailingStatefulOperator(calls);
    try {
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> {
                try (var harness =
                    new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, value -> value, Types.INT, 128, 1, 0)) {
                  harness.open();
                }
              });
      assertEquals("state configuration failed", failure.getMessage());
      assertTrue(failure.getSuppressed().length > 0);
      assertEquals(List.of("handle", "binding"), calls);
      assertEquals(0, operator.liveHandle());
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
      operator.close();
      assertEquals(2, calls.size());
    } finally {
      TaskOffHeapMemory.resetForTests(previousCapacity == 0 ? -1 : previousCapacity);
    }
  }

  private static final class FailingStatefulOperator extends AbstractNativeStatefulOperator<Integer>
      implements OneInputStreamOperator<Integer, Integer> {
    private final List<String> calls;

    FailingStatefulOperator(List<String> calls) {
      super("cleanup-test", new int[0], 128);
      this.calls = calls;
    }

    @Override
    protected void beforeHandleCreation() throws Exception {
      ownResource(() -> calls.add("binding"));
      assertTrue(TaskOffHeapMemory.tryReserve(-memoryBudgetBytes() - 1, 256));
    }

    @Override
    protected long createHandle() {
      return 1;
    }

    @Override
    protected long restoreRawHandle(byte[][] snapshots) {
      return createHandle();
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      return new byte[0][];
    }

    @Override
    protected long stateBytesHandle() {
      return 256;
    }

    @Override
    public void processElement(StreamRecord<Integer> record) {}

    @Override
    protected void configureStateHandle() {
      throw new IllegalStateException("state configuration failed");
    }

    @Override
    protected void closeHandle() {
      calls.add("handle");
      throw new IllegalStateException("handle cleanup failed");
    }

    long liveHandle() {
      return handle;
    }
  }
}
