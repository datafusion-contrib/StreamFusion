package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class CanonicalManagedUdfWideKeyStateTest {
  static Stream<Arguments> switches() {
    return Arrays.stream(ManagedUdfKeyShape.values())
        .flatMap(
            shape ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                first ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            cross ->
                                                                Arguments.of(
                                                                    shape, mode, rocks, first,
                                                                    cross))))));
  }

  @ParameterizedTest(name = "{0}:mode={1},rocks={2},nativeFirst={3},changeBackend={4}")
  @MethodSource("switches")
  void actualKeyAndAllAccumulatorViewBytesSurviveThreeStageSwitches(
      ManagedUdfKeyShape shape, int mode, boolean rocks, boolean first, boolean cross)
      throws Exception {
    OperatorSubtaskState saved = null, referenceSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean store = cross && stage == 1 ? !rocks : rocks;
      boolean nativeRun = stage % 2 == 0 ? first : !first;
      try (var reference = fixture(false, shape, mode, store, 0);
          var receiver = fixture(nativeRun, shape, mode, store, 0)) {
        reference.open(referenceSaved, 1000 + stage * 100);
        receiver.open(saved, 1000 + stage * 100);
        if (saved != null) {
          assertEquals(reference.rawState(), receiver.rawState(), "before new input");
          try (var immediate = fixture(!nativeRun, shape, mode, store, 0)) {
            immediate.open(receiver.canonical(), 1000 + stage * 100);
            assertEquals(
                reference.rawState(), immediate.rawState(), "no-input immediate re-export");
          }
        }
        assertEquals(
            reference.feed(shape.input(stage), false),
            receiver.feed(shape.input(stage), stage % 2 == 0));
        var expected = reference.rawState();
        assertTrue(expected.keySet().stream().anyMatch(k -> k.startsWith("accState:")));
        assertTrue(expected.keySet().stream().anyMatch(k -> k.contains("counts")));
        assertTrue(expected.keySet().stream().anyMatch(k -> k.contains("history")));
        assertEquals(expected, receiver.rawState(), "all original key, accumulator and view bytes");
        saved = receiver.canonical();
        referenceSaved = reference.canonical();
      }
    }
  }

  static Stream<Arguments> ttl() {
    return Arrays.stream(ManagedUdfKeyShape.values())
        .flatMap(
            shape ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        mode ->
                            Stream.of(false, true).map(rocks -> Arguments.of(shape, mode, rocks))));
  }

  @ParameterizedTest(name = "ttl:{0}:mode={1},rocks={2}")
  @MethodSource("ttl")
  void fractionalAndNullableKeysRetainOriginalTtlAges(
      ManagedUdfKeyShape shape, int mode, boolean rocks) throws Exception {
    OperatorSubtaskState saved, referenceSaved;
    Map<String, String> original;
    try (var source = fixture(false, shape, mode, rocks, 1000)) {
      source.open(null, 1000);
      source.feed(shape.input(0), false);
      original = source.rawState();
      referenceSaved = saved = source.canonical();
    }
    for (int i = 0; i < 2; i++) {
      try (var next = fixture(i == 0, shape, mode, rocks, 1000)) {
        next.open(saved, i == 0 ? 1500 : 1999);
        if (next.nativeOwner != null)
          assertNull(field(next.nativeOwner.getOperator(), "accumulatorCache"));
        assertEquals(original, next.rawState(), "TTL timestamps must not refresh");
        saved = next.canonical();
      }
    }
    try (var reference = fixture(false, shape, mode, rocks, 1000);
        var receiver = fixture(true, shape, mode, rocks, 1000)) {
      reference.open(referenceSaved, 2001);
      receiver.open(saved, 2001);
      assertEquals(reference.feed(shape.input(0), false), receiver.feed(shape.input(0), true));
      assertEquals(reference.rawState(), receiver.rawState());
    }
  }

  private static ManagedUdfGroupStockFixture fixture(
      boolean nativeRun, ManagedUdfKeyShape shape, int mode, boolean rocks, long ttl)
      throws Exception {
    return new ManagedUdfGroupStockFixture(
        nativeRun, mode > 0, rocks, 16, ttl, mode == 2, false, shape.keys);
  }
}
