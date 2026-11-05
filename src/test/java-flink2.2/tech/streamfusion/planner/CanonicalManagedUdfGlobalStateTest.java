package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.row;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class CanonicalManagedUdfGlobalStateTest extends CanonicalManagedUdfGroupStateTest {
  @Override
  ManagedUdfGroupStockFixture fixture(
      boolean nativeRun, boolean mini, boolean rocks, int cache, long ttl) throws Exception {
    return new ManagedUdfGroupStockFixture(nativeRun, true, rocks, cache, ttl, true, false);
  }

  static Stream<Arguments> switches() {
    return CanonicalManagedUdfGroupStateTest.switches().filter(args -> (boolean) args.get()[0]);
  }

  static Stream<Arguments> ttlCases() {
    return CanonicalManagedUdfGroupStateTest.ttlCases().filter(args -> (boolean) args.get()[0]);
  }

  static Stream<Arguments> failures() {
    return Stream.of(false, true)
        .flatMap(
            local ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0, 16).map(cache -> Arguments.of(local, rocks, cache))));
  }

  @Override
  @ParameterizedTest(name = "fault:local={0},rocks={1},cache={2}")
  @MethodSource("failures")
  void failedUserFunctionCannotReplaceTheLastSuccessfulSavepoint(
      boolean local, boolean rocks, int cache) throws Exception {
    OperatorSubtaskState saved;
    try (var initial = faulting(false, rocks, 0)) {
      initial.open(null, 1000);
      initial.feed(List.of(row(1L, 7L, 0)), false);
      saved = initial.canonical();
    }
    try (var failing = faulting(true, rocks, cache)) {
      failing.open(saved, 1100);
      var failure =
          assertThrows(
              Exception.class,
              () ->
                  failing.feed(
                      List.of(row(1L, local ? Long.MIN_VALUE : Long.MAX_VALUE, 0)), false));
      boolean expected = false;
      String message = local ? "Injected local UDF failure" : "Injected global UDF failure";
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        expected |= String.valueOf(cause.getMessage()).contains(message);
      assertTrue(expected, failure.toString());
      failing.cancel();
    }
    try (var reference = faulting(false, rocks, 0);
        var recovered = faulting(true, rocks, cache)) {
      reference.open(saved, 1200);
      recovered.open(saved, 1200);
      assertEquals(reference.rawState(), recovered.rawState());
      var input = List.<org.apache.flink.table.data.RowData>of(row(1L, 2L, 0));
      assertEquals(reference.feed(input, false), recovered.feed(input, true));
      assertEquals(reference.rawState(), recovered.rawState());
    }
  }

  static Stream<Arguments> checkpoints() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(0, 16)
                    .flatMap(
                        cache ->
                            Stream.of(false, true)
                                .map(split -> Arguments.of(rocks, cache, split))));
  }

  @ParameterizedTest(name = "checkpoint:rocks={0},cache={1},singleRows={2}")
  @MethodSource("checkpoints")
  void ordinaryRecoveryRetainsOriginalGlobalViewsAndLocalBundleSemantics(
      boolean rocks, int cache, boolean split) throws Exception {
    OperatorSubtaskState checkpoint, referenceSaved;
    try (var reference = fixture(false, true, rocks, 0, 0);
        var source = fixture(true, true, rocks, cache, 0)) {
      reference.open(null, 1000);
      source.open(null, 1000);
      assertEquals(reference.feed(seeds(0), false), source.feed(seeds(0), split));
      assertEquals(reference.rawState(), source.rawState());
      checkpoint = source.checkpoint();
      referenceSaved = reference.canonical();
    }
    try (var reference = fixture(false, true, rocks, 0, 0);
        var restored = fixture(true, true, rocks, cache, 0)) {
      reference.open(referenceSaved, 1200);
      restored.open(checkpoint, 1200);
      assertEquals(reference.rawState(), restored.rawState());
      assertEquals(reference.feed(seeds(1), false), restored.feed(seeds(1), split));
      assertEquals(reference.rawState(), restored.rawState());
      checkpoint = restored.canonical();
      referenceSaved = reference.canonical();
    }
    try (var reference = fixture(false, true, rocks, 0, 0);
        var returned = fixture(false, true, rocks, 0, 0)) {
      reference.open(referenceSaved, 1300);
      returned.open(checkpoint, 1300);
      assertEquals(reference.rawState(), returned.rawState());
      assertEquals(reference.feed(seeds(2), false), returned.feed(seeds(2), false));
      assertEquals(reference.rawState(), returned.rawState());
    }
  }

  private ManagedUdfGroupStockFixture faulting(boolean nativeRun, boolean rocks, int cache)
      throws Exception {
    return new ManagedUdfGroupStockFixture(nativeRun, true, rocks, cache, 0, true, true);
  }
}
