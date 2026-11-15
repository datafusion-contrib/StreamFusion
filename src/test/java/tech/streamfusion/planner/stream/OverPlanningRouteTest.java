package tech.streamfusion.planner.stream;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OverPlanningRouteTest {
  static Stream<Arguments> frames() {
    return Stream.of("processing-rows", "event-rows", "event-range",
        "processing-unbounded-rows", "processing-unbounded-range",
        "event-unbounded-rows", "event-unbounded-range")
        .flatMap(mode -> Stream.of(false, true).flatMap(compiled ->
            Stream.of(false, true).map(keyless -> Arguments.of(mode, compiled, keyless))));
  }

  @ParameterizedTest
  @MethodSource("frames")
  void sharedFrameSemanticsRetainActualStockStateAndIdentity(
      String mode, boolean compiled, boolean keyless) throws Exception {
    new CanonicalBoundedOverPlanTest().assertPlan(mode, "BIGINT", keyless, compiled, "ALWAYS", 0, false);
  }
}
