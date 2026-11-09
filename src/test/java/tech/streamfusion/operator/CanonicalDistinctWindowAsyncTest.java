package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalProcessingTimeWindowOperatorTest.*;
import static tech.streamfusion.operator.CanonicalSharedWindowInteropTest.BASE;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.CanonicalTestCompat;

class CanonicalDistinctWindowAsyncTest {
  @BeforeAll
  static void initializeMemory() {
    memoryAuthority();
  }

  static Stream<Arguments> cases() {
    return Stream.of("TUMBLE", "HOP", "CUMULATE", "SESSION")
        .flatMap(
            shape ->
                Stream.of(false, true)
                    .flatMap(
                        keyless ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(shape, keyless, rocks))));
  }

  @ParameterizedTest(name = "{0},keyless={1},rocks={2}")
  @MethodSource("cases")
  void independentDistinctViewsConvertAfterLiveWritesAndNativeOwnerClose(
      String shape, boolean keyless, boolean rocks) throws Exception {
    var plan = new CanonicalSharedWindowInteropTest.StockPlan(shape, "UTC", keyless, false, true);
    var captured = new OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    try (var stock = plan.stock(true);
        var source = nativeHarness(plan, rocks)) {
      try {
        stock.open();
        source.open();
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertEquals(
            rocks, owner.directRocksDBState(), "capture must use the requested native store");
        for (int stage = 0; stage < 3; stage++) {
          for (int index = 0; index < 3; index++) {
            var row =
                input(index == 0 ? null : 7L, (long) (stage * 10 + index), 100 + stage * 200L);
            plan.send(stock, row);
            send(plan, source, row);
          }
          if (stage < 2) {
            stock.prepareSnapshotPreBarrier(10 + stage);
            expected[stage] =
                stock
                    .snapshotWithLocalState(
                        10 + stage, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
            captured[stage] = capture(source, 10 + stage);
          }
        }
        source.prepareSnapshotPreBarrier(12);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = finishOnWriter(captured[stage]);
          try (var reference = plan.stock(true);
              var returned = plan.stock(true)) {
            reference.initializeState(expected[stage]);
            returned.initializeState(saved);
            reference.open();
            returned.open();
            reference.processWatermark(new Watermark(BASE + 10000));
            returned.processWatermark(new Watermark(BASE + 10000));
            assertEquals(plan.output(reference), plan.output(returned));
          }
        }
      } finally {
        for (var futures : captured) if (futures != null) futures.cancel();
      }
    }
  }

  static OperatorSubtaskState finishOnWriter(OperatorSnapshotFutures futures) throws Exception {
    assertFalse(futures.getKeyedStateManagedFuture().isDone());
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                return CanonicalTestCompat.finishSnapshot(futures);
              } catch (Exception failure) {
                throw new CompletionException(failure);
              }
            })
        .get(30, TimeUnit.SECONDS);
  }

  static OperatorSnapshotFutures capture(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> source, long id)
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
