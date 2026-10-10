package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tech.streamfusion.compat.RichAsyncFunction;

@Timeout(10)
class NativeAsyncLookupJoinOperatorTest {
  private static final RowType TYPE = RowType.of(new BigIntType());

  @Test
  void capacityBoundsOutstandingLookupsAndOutputRetainsProbeOrder() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    ExecutorService task = Executors.newSingleThreadExecutor();
    try (var harness = harness(lookup, 2, 5000)) {
      harness.open();
      Future<?> processing =
          task.submit(
              () -> {
                process(harness, 1, 2, 3);
                return null;
              });
      Call first = lookup.next();
      Call second = lookup.next();
      assertEquals(1, first.row().getLong(0));
      assertEquals(2, second.row().getLong(0));
      assertNull(lookup.calls.poll(30, TimeUnit.MILLISECONDS));

      second.complete();
      first.complete();
      Call third = lookup.next();
      assertEquals(3, third.row().getLong(0));
      third.complete();
      processing.get(5, TimeUnit.SECONDS);

      assertEquals(List.of(1L, 2L, 3L), output(harness));
      assertEquals(1, lookup.opens);
    } finally {
      task.shutdownNow();
    }
    assertEquals(1, lookup.closes);
  }

  @Test
  void timeoutUsesFlinksFailureWithoutFillingTheRunnerBufferIndefinitely() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    try (var harness = harness(lookup, 2, 30)) {
      harness.open();
      TimeoutException error =
          assertThrows(TimeoutException.class, () -> process(harness, 1, 2, 3));
      assertEquals("Async function call has timed out.", error.getMessage());
      assertEquals(2, lookup.calls.size());
      assertTrue(harness.getOutput().isEmpty());
      lookup.next().complete();
      lookup.next().complete();
      assertTrue(harness.getOutput().isEmpty(), "late callbacks cannot emit a failed batch");
    }
    assertEquals(1, lookup.closes);
  }

  @Test
  void failureInALaterProbeStopsWaitingForAnEarlierOne() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    ExecutorService task = Executors.newSingleThreadExecutor();
    try (var harness = harness(lookup, 2, 0)) {
      harness.open();
      Future<?> processing =
          task.submit(
              () -> {
                process(harness, 1, 2, 3);
                return null;
              });
      Call first = lookup.next();
      Call second = lookup.next();
      IOException failure = new IOException("connector lookup failed");
      second.result().completeExceptionally(failure);

      ExecutionException error =
          assertThrows(ExecutionException.class, () -> processing.get(5, TimeUnit.SECONDS));
      assertSame(failure, error.getCause());
      assertTrue(lookup.calls.isEmpty(), "failure must stop admission of additional probes");
      first.complete();
      assertTrue(harness.getOutput().isEmpty());
    } finally {
      task.shutdownNow();
    }
    assertEquals(1, lookup.closes);
  }

  @Test
  void zeroTimeoutKeepsTheHostDisabledTimeoutPolicy() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    ExecutorService task = Executors.newSingleThreadExecutor();
    try (var harness = harness(lookup, 1, 0)) {
      harness.open();
      Future<?> processing =
          task.submit(
              () -> {
                process(harness, 4);
                return null;
              });
      Call call = lookup.next();
      assertFalse(processing.isDone());
      call.complete();
      processing.get(5, TimeUnit.SECONDS);
      assertEquals(List.of(4L), output(harness));
    } finally {
      task.shutdownNow();
    }
  }

  @Test
  void interruptionAbandonsTheBatchAndPreservesTheTaskInterrupt() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    ExecutorService task = Executors.newSingleThreadExecutor();
    CountDownLatch stopped = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    try (var harness = harness(lookup, 1, 0)) {
      harness.open();
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      Future<?> processing =
          task.submit(
              () -> {
                try {
                  process(harness, 8, 9);
                  return null;
                } catch (InterruptedException expected) {
                  interrupted.set(Thread.currentThread().isInterrupted());
                  return null;
                } finally {
                  stopped.countDown();
                }
              });
      Call pending = lookup.next();
      processing.cancel(true);
      assertTrue(stopped.await(5, TimeUnit.SECONDS));
      assertTrue(interrupted.get());
      pending.complete();
      assertTrue(lookup.calls.isEmpty());
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    } finally {
      task.shutdownNow();
    }
    assertEquals(1, lookup.closes);
  }

  @Test
  void failedOpenClosesThePartiallyOpenedLookupExactlyOnce() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    lookup.openFailure = new IOException("opening connector");
    lookup.closeFailure = new IOException("closing connector");
    try (var harness = harness(lookup, 1, 5000)) {
      IOException error = assertThrows(IOException.class, harness::open);
      assertSame(lookup.openFailure, error);
      assertEquals(List.of(lookup.closeFailure), List.of(error.getSuppressed()));
      assertEquals(1, lookup.closes);
    }
    assertEquals(1, lookup.closes);
  }

  @Test
  void synchronousInvokeFailureClosesTheInputAndDoesNotEmit() throws Exception {
    ControlledLookup lookup = new ControlledLookup();
    lookup.invokeFailure = new IOException("invoking connector");
    try (var harness = harness(lookup, 1, 5000)) {
      harness.open();
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      IOException error = assertThrows(IOException.class, () -> process(harness, 3));
      assertSame(lookup.invokeFailure, error);
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
      assertTrue(harness.getOutput().isEmpty());
    }
    assertEquals(1, lookup.closes);
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness(
      ControlledLookup lookup, int capacity, long timeout) throws Exception {
    var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new NativeAsyncLookupJoinOperator(lookup, TYPE, TYPE, capacity, timeout));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static void process(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness, long... values)
      throws Exception {
    List<RowData> rows = new ArrayList<>();
    for (long value : values) rows.add(GenericRowData.of(value));
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(rows, TYPE, NativeAllocator.SHARED, false))));
  }

  private static List<Long> output(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<Long> values = new ArrayList<>();
    for (var record : harness.extractOutputStreamRecords()) {
      try (VectorSchemaRoot root = record.getValue().root()) {
        for (RowData row : RowDataArrowConverter.read(root, TYPE)) values.add(row.getLong(0));
      }
    }
    harness.getOutput().clear();
    return values;
  }

  private static final class Call {
    private final RowData row;
    private final ResultFuture<RowData> result;

    private Call(RowData row, ResultFuture<RowData> result) {
      this.row = row;
      this.result = result;
    }

    public RowData row() {
      return row;
    }

    public ResultFuture<RowData> result() {
      return result;
    }

    void complete() {
      result.complete(List.of(row));
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Call that = (Call) other;
      return java.util.Objects.equals(row, that.row)
          && java.util.Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(row);
      result = 31 * result + java.util.Objects.hashCode(result);
      return result;
    }

    @Override
    public String toString() {
      return "Call[row=" + row + ", result=" + result + "]";
    }
  }

  private static final class ControlledLookup extends RichAsyncFunction<RowData, RowData> {
    final BlockingQueue<Call> calls = new LinkedBlockingQueue<>();
    int opens;
    int closes;
    IOException openFailure;
    IOException closeFailure;
    IOException invokeFailure;

    @Override
    protected void initialize() throws Exception {
      opens++;
      if (openFailure != null) throw openFailure;
    }

    @Override
    public void asyncInvoke(RowData row, ResultFuture<RowData> result) throws Exception {
      if (invokeFailure != null) throw invokeFailure;
      calls.add(new Call(row, result));
    }

    @Override
    public void close() throws Exception {
      closes++;
      if (closeFailure != null) throw closeFailure;
    }

    Call next() throws InterruptedException {
      Call call = calls.poll(5, TimeUnit.SECONDS);
      if (call == null) throw new AssertionError("lookup did not start");
      return call;
    }
  }
}
