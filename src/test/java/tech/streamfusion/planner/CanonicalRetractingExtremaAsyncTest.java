package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;
import static tech.streamfusion.planner.CanonicalTypedRetractingExtremaTest.fixture;
import static tech.streamfusion.planner.CanonicalTypedRetractingExtremaTest.row;
import static tech.streamfusion.planner.CanonicalTypedRetractingExtremaTest.values;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.CanonicalTestCompat;
import tech.streamfusion.operator.ArrowBatch;

class CanonicalRetractingExtremaAsyncTest {
  static Stream<Arguments> cases() {
    return CanonicalTypedRetractingExtremaTest.types()
        .flatMap(type -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(type, ttl)));
  }

  @ParameterizedTest(name = "{0},ttl={1}")
  @MethodSource("cases")
  void independentCapturesConvertAfterLiveWritesAndOwnerClose(String type, long ttl)
      throws Exception {
    OperatorSnapshotFutures first = null, second = null;
    Map<String, String> earlier = null, later = null;
    try (var reference = fixture(type, true, ttl);
        var source = fixture(type, true, ttl);
        var target = nativeHarness(source, true)) {
      try {
        reference.harness.open();
        target.open();
        var owner = (DirectGroup) target.getOperator();
        assertTrue(owner.persistent(), "must exercise native RocksDB");
        var v = values(type);
        for (int stage = 0; stage < 3; stage++) {
          long time = 1000 + 500L * stage;
          for (Object value : new Object[] {v[stage], v[stage], null}) {
            var next = row(1L, value, 0);
            assertEquals(
                stockStep(reference, next, time),
                nativeStep(target, next, time, source.outputType, source.inputType));
          }
          if (stage == 0) {
            first = capture(target, 10);
            earlier = reference.rawStateBytes();
          } else if (stage == 1) {
            second = capture(target, 11);
            later = reference.rawStateBytes();
          }
        }
        owner.closeNativeOwner();
        assertNotNull(earlier);
        assertNotNull(later);
        assertContents(later, finishOnWriter(second), type, ttl);
        assertContents(earlier, finishOnWriter(first), type, ttl);
      } finally {
        if (first != null) first.cancel();
        if (second != null) second.cancel();
      }
    }
  }

  private static org.apache.flink.runtime.checkpoint.OperatorSubtaskState finishOnWriter(
      OperatorSnapshotFutures captured) throws Exception {
    assertFalse(captured.getKeyedStateManagedFuture().isDone());
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                return CanonicalTestCompat.finishSnapshot(captured);
              } catch (Exception failure) {
                throw new CompletionException(failure);
              }
            })
        .get(30, TimeUnit.SECONDS);
  }

  private static void assertContents(
      Map<String, String> expected,
      org.apache.flink.runtime.checkpoint.OperatorSubtaskState saved,
      String type,
      long ttl)
      throws Exception {
    try (var stock = fixture(type, true, ttl)) {
      stock.harness.initializeState(saved);
      stock.harness.open();
      assertEquals(expected, stock.rawStateBytes());
    }
  }

  private static OperatorSnapshotFutures capture(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> source, long id)
      throws Exception {
    source.prepareSnapshotPreBarrier(id);
    return source
        .getOperator()
        .snapshotState(
            id,
            0,
            CheckpointOptions.forConfig(
                SavepointType.savepoint(SavepointFormatType.CANONICAL),
                CheckpointStorageLocationReference.getDefault(),
                true,
                false,
                0),
            new MemCheckpointStreamFactory(16 * 1024 * 1024));
  }
}
