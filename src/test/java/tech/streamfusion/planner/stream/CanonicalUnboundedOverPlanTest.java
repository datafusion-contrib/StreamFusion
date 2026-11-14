package tech.streamfusion.planner.stream;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalUnboundedOverPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    return Stream.of("processing-unbounded-rows", "processing-unbounded-range")
        .flatMap(
            mode ->
                Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
                    "DECIMAL(18,2)", "DECIMAL(38,2)", "DECIMAL(38,20)")
                    .flatMap(
                        type ->
                            Stream.of(false, true)
                                .flatMap(
                                    global ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                compiled ->
                                                    Stream.of("DISABLED", "ALWAYS")
                                                        .flatMap(
                                                            uids ->
                                                                Stream.of(0L, 1000L)
                                                                    .map(
                                                                        ttl ->
                                                                            Arguments.of(
                                                                                mode, type, global,
                                                                                compiled, uids,
                                                                                ttl)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionCapturesRawUnboundedStateAndOriginalStockIdentity(
      String mode, String type, boolean global, boolean compiled, String uids, long ttl)
      throws Exception {
    new CanonicalBoundedOverPlanTest().assertPlan(mode, type, global, compiled, uids, ttl, false);
  }

  static Stream<Arguments> constantCases() {
    return Stream.of("processing-unbounded-rows", "processing-unbounded-range")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        global ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of("DISABLED", "ALWAYS")
                                            .flatMap(
                                                uids ->
                                                    Stream.of(0L, 1000L)
                                                        .map(
                                                            ttl ->
                                                                Arguments.of(
                                                                    mode, global, compiled, uids,
                                                                    ttl))))));
  }

  @ParameterizedTest
  @MethodSource("constantCases")
  void countStarAndLiteralsRetainTheirActualConstantColumns(
      String mode, boolean global, boolean compiled, String uids, long ttl) throws Exception {
    new CanonicalBoundedOverPlanTest()
        .assertPlan(mode, "BIGINT", global, compiled, uids, ttl, true);
  }
}
