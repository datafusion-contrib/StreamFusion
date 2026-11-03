package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupArithmeticRecoveryTest {
  static Stream<Arguments> cases() {
    return Stream.of("arithmetic", "hidden")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(mode, rocks, ttl))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void ordinaryNativeCheckpointResumesBeforeStockCanonicalExport(
      String mode, boolean rocks, long ttl) throws Exception {
    try (var reference = new GroupAggregateStockTestFixture(mode, rocks, ttl, true)) {
      reference.harness.open();
      OperatorSubtaskState checkpoint;
      try (var fixture = new GroupAggregateStockTestFixture(mode, rocks, ttl, true);
          var source = nativeHarness(fixture, rocks)) {
        source.open();
        for (int i = 0; i < 3; i++) {
          var input = row(1L, i == 0 ? null : i == 1 ? 7L : 4L, 0);
          assertEquals(
              stockStep(reference, input, 1000 + 100L * i),
              nativeStep(source, input, 1000 + 100L * i, reference.outputType));
        }
        var owner = (DirectGroup) source.getOperator();
        owner.checkpoint = true;
        try {
          source.prepareSnapshotPreBarrier(10);
          checkpoint = source.snapshot(10, 1200);
        } finally {
          owner.checkpoint = false;
        }
      }
      OperatorSubtaskState canonical;
      long resumed = ttl == 0 ? 5000 : 1500;
      try (var fixture = new GroupAggregateStockTestFixture(mode, rocks, ttl, true);
          var restored = nativeHarness(fixture, rocks)) {
        restored.setProcessingTime(resumed);
        restored.setStateTtlProcessingTime(resumed);
        restored.initializeState(checkpoint);
        restored.open();
        // Verify ages immediately after raw restoration, including TTL-off migration clocks.
        assertState(reference, snapshot(restored), mode, rocks, ttl, true);
        assertEquals(
            stockStep(reference, row(1L, 3L, 0), resumed + 50),
            nativeStep(restored, row(1L, 3L, 0), resumed + 50, reference.outputType));
        canonical = snapshot(restored);
        assertState(reference, canonical, mode, rocks, ttl, true);
      }
      try (var returned = new GroupAggregateStockTestFixture(mode, rocks, ttl, true)) {
        returned.harness.initializeState(canonical);
        returned.harness.open();
        assertEquals(
            stockStep(reference, row(1L, 7L, 3), resumed + 100),
            stockStep(returned, row(1L, 7L, 3), resumed + 100));
        assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
      }
    }
  }
}
