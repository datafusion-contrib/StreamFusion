package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.row;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class CanonicalManagedUdfGroupStateTest {
  static Stream<Arguments> switches() {
    return Stream.of(false, true)
        .flatMap(
            mini ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .flatMap(
                                    first ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                cross ->
                                                    Stream.of(0, 16)
                                                        .flatMap(
                                                            cache ->
                                                                Stream.of(false, true)
                                                                    .map(
                                                                        split ->
                                                                            Arguments.of(
                                                                                mini, rocks, first,
                                                                                cross, cache,
                                                                                split)))))));
  }

  @ParameterizedTest(name = "mini={0},rocks={1},nativeFirst={2},cross={3},cache={4},singleRows={5}")
  @MethodSource("switches")
  void completeManagedStateSurvivesRepeatedSwitchesAndImmediateReexport(
      boolean mini, boolean rocks, boolean first, boolean cross, int cache, boolean split)
      throws Exception {
    OperatorSubtaskState saved = null, referenceSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean store = cross && stage % 2 == 1 ? !rocks : rocks;
      boolean nativeRun = stage % 2 == 0 ? first : !first;
      try (var reference = new ManagedUdfGroupStockFixture(false, mini, store, 0, 0);
          var receiver = new ManagedUdfGroupStockFixture(nativeRun, mini, store, cache, 0)) {
        reference.open(referenceSaved, 1000 + stage * 100);
        receiver.open(saved, 1000 + stage * 100);
        if (saved != null) {
          assertEquals(reference.rawState(), receiver.rawState(), "raw state before any new input");
          saved = receiver.canonical();
          try (var immediate = new ManagedUdfGroupStockFixture(!nativeRun, mini, store, cache, 0)) {
            immediate.open(saved, 1000 + stage * 100);
            assertEquals(
                reference.rawState(),
                immediate.rawState(),
                "immediate re-export preserves views and hidden fields");
            saved = immediate.canonical();
          }
        }
        assertEquals(reference.feed(seeds(stage), false), receiver.feed(seeds(stage), split));
        var expected = reference.rawState();
        assertTrue(expected.keySet().stream().anyMatch(key -> key.startsWith("accState:")));
        assertTrue(
            expected.keySet().stream().anyMatch(key -> key.contains("counts")),
            expected.toString());
        assertTrue(
            expected.keySet().stream().anyMatch(key -> key.contains("history")),
            expected.toString());
        assertEquals(expected, receiver.rawState(), "all canonical KV bytes, including both views");
        saved = receiver.canonical();
        referenceSaved = reference.canonical();
      }
    }
  }

  static Stream<Arguments> ttlCases() {
    return Stream.of(false, true)
        .flatMap(
            mini ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .flatMap(
                                    first ->
                                        Stream.of(0, 16)
                                            .map(
                                                cache ->
                                                    Arguments.of(mini, rocks, first, cache)))));
  }

  @ParameterizedTest(name = "ttl:mini={0},rocks={1},nativeFirst={2},cache={3}")
  @MethodSource("ttlCases")
  void noInputSwitchesPreserveOriginalTtlAgesAndExpiredViewsAreNotResurrected(
      boolean mini, boolean rocks, boolean first, int cache) throws Exception {
    OperatorSubtaskState saved, referenceSaved;
    Map<String, String> original;
    try (var reference = new ManagedUdfGroupStockFixture(false, mini, rocks, 0, 1000);
        var receiver = new ManagedUdfGroupStockFixture(first, mini, rocks, cache, 1000)) {
      reference.open(null, 1000);
      receiver.open(null, 1000);
      var initial = List.<RowData>of(row(1L, 7L, 0), row(1L, null, 0), row(2L, 5L, 0));
      assertEquals(reference.feed(initial, false), receiver.feed(initial, false));
      original = reference.rawState();
      assertEquals(original, receiver.rawState());
      saved = receiver.canonical();
      referenceSaved = reference.canonical();
    }
    for (int stage = 0; stage < 2; stage++) {
      try (var receiver =
          new ManagedUdfGroupStockFixture(stage == 0 ? !first : first, mini, rocks, cache, 1000)) {
        receiver.open(saved, stage == 0 ? 1500 : 1999);
        if (receiver.nativeOwner != null)
          assertNull(
              field(receiver.nativeOwner.getOperator(), "accumulatorCache"),
              "TTL must bypass caching");
        assertEquals(
            original, receiver.rawState(), "canonical handoffs must not refresh TTL timestamps");
        saved = receiver.canonical();
      }
    }
    try (var reference = new ManagedUdfGroupStockFixture(false, mini, rocks, 0, 1000);
        var receiver = new ManagedUdfGroupStockFixture(!first, mini, rocks, cache, 1000)) {
      reference.open(referenceSaved, 2001);
      receiver.open(saved, 2001);
      var fresh = List.<RowData>of(row(1L, 3L, 0), row(2L, 4L, 0));
      var expected = reference.feed(fresh, false);
      assertEquals(expected, receiver.feed(fresh, true));
      assertEquals(reference.rawState(), receiver.rawState());
      assertEquals(Set.of("1", "2"), expected.keySet());
    }
  }

  static Stream<Arguments> failures() {
    return Stream.of(false, true)
        .flatMap(
            mini ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0, 16).map(cache -> Arguments.of(mini, rocks, cache))));
  }

  @ParameterizedTest(name = "failure:mini={0},rocks={1},cache={2}")
  @MethodSource("failures")
  void failedUserFunctionCannotReplaceTheLastSuccessfulSavepoint(
      boolean mini, boolean rocks, int cache) throws Exception {
    OperatorSubtaskState saved;
    try (var initial = new ManagedUdfGroupStockFixture(false, mini, rocks, 0, 0)) {
      initial.open(null, 1000);
      initial.feed(List.of(row(1L, 7L, 0)), false);
      saved = initial.canonical();
    }
    try (var failing = new ManagedUdfGroupStockFixture(true, mini, rocks, cache, 0)) {
      failing.open(saved, 1100);
      var failure =
          assertThrows(Exception.class, () -> failing.feed(List.of(row(1L, 17L, 3)), false));
      boolean expected = false;
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        expected |= String.valueOf(cause.getMessage()).contains("Missing history value");
      assertTrue(expected, failure.toString());
      failing.cancel();
    }
    try (var reference = new ManagedUdfGroupStockFixture(false, mini, rocks, 0, 0);
        var recovered = new ManagedUdfGroupStockFixture(true, mini, rocks, cache, 0)) {
      reference.open(saved, 1200);
      recovered.open(saved, 1200);
      assertEquals(reference.rawState(), recovered.rawState());
      assertEquals(
          reference.feed(List.of(row(1L, 2L, 0)), false),
          recovered.feed(List.of(row(1L, 2L, 0)), false));
      assertEquals(reference.rawState(), recovered.rawState());
    }
  }

  private static List<RowData> seeds(int stage) {
    switch (stage) {
      case 0:
        return List.of(
            row(1L, null, 0),
            row(1L, 7L, 0),
            row(1L, 7L, 0),
            row(2L, 11L, 0),
            row(null, 5L, 0),
            row(3L, null, 0),
            row(2L, 2L, 0));
      case 1:
        return List.of(
            row(1L, 7L, 1),
            row(1L, 8L, 2),
            row(2L, 11L, 3),
            row(null, 5L, 3),
            row(3L, null, 3),
            row(1L, null, 3),
            row(3L, 4L, 0));
      case 2:
        return List.of(
            row(1L, 7L, 3),
            row(1L, 8L, 3),
            row(3L, 4L, 3),
            row(null, null, 0),
            row(1L, -5L, 0),
            row(1L, 2L, 0),
            row(null, 6L, 0));
      default:
        throw new IllegalArgumentException();
    }
  }
}
