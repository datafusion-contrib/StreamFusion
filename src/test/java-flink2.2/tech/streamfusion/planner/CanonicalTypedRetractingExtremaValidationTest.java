package tech.streamfusion.planner;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalTypedRetractingExtremaValidationTest {
  static Stream<Arguments> modes() {
    return CanonicalTypedRetractingExtremaTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(type, rocks, ttl))));
  }

  static Stream<Arguments> invalidSources() {
    return modes()
        .flatMap(
            args ->
                Stream.of(
                        "late-null-count",
                        "late-null-view",
                        "bad-placeholder",
                        "row-kind",
                        "foreign")
                    .map(
                        reason ->
                            Arguments.of(args.get()[0], args.get()[1], args.get()[2], reason)));
  }

  static Stream<Arguments> receivers() {
    return modes()
        .flatMap(
            args ->
                Stream.of(false, true)
                    .map(
                        entries ->
                            Arguments.of(args.get()[0], args.get()[1], args.get()[2], entries)));
  }

  private static CanonicalRetractingExtremaValidationTest validator(String type) {
    return new CanonicalRetractingExtremaValidationTest() {
      @Override
      GroupAggregateStockTestFixture fixture(String mode, boolean rocks, long ttl)
          throws Exception {
        return CanonicalTypedRetractingExtremaTest.fixture(type, rocks, ttl);
      }
    };
  }

  @ParameterizedTest(name = "invalid:{0},rocks={1},ttl={2},reason={3}")
  @MethodSource("invalidSources")
  void lateCorruptionKeepsAllSourceEntriesAndAnEmptyTarget(
      String type, boolean rocks, long ttl, String reason) throws Exception {
    validator(type)
        .completeValidationKeepsCorruptSourceAndEmptyNativeTarget("extrema", rocks, ttl, reason);
  }

  @ParameterizedTest(name = "denied:{0},rocks={1},ttl={2}")
  @MethodSource("modes")
  void deniedAllocationKeepsSourceForFreshRetry(String type, boolean rocks, long ttl)
      throws Exception {
    validator(type).deniedImportRetainsAllHeadersAndViewsForFreshRetry("extrema", rocks, ttl);
  }

  @ParameterizedTest(name = "receiver:{0},rocks={1},ttl={2},entries={3}")
  @MethodSource("receivers")
  void receiverFailureReleasesBuffersAndKeepsLiveState(
      String type, boolean rocks, long ttl, boolean entries) throws Exception {
    validator(type).receiverFailurePreservesExceptionAndLiveState("extrema", rocks, ttl, entries);
  }
}
