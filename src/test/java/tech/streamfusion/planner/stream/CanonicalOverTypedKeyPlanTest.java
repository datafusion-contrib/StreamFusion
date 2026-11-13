package tech.streamfusion.planner.stream;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.operator.CanonicalOverKeyFixture;

class CanonicalOverTypedKeyPlanTest {
  static Stream<Arguments> keys() {
    return CanonicalOverKeyFixture.types()
        .flatMap(type -> Stream.of(false, true)
            .flatMap(composite -> Stream.of(false, true)
                .flatMap(compiled -> Stream.of(
                    "processing-rows", "event-rows", "event-range", "processing-unbounded-rows",
                    "event-unbounded-rows", "event-unbounded-range")
                    .map(mode -> Arguments.of(type, composite, compiled, mode)))));
  }

  @ParameterizedTest
  @MethodSource("keys")
  void existingScalarKeyCarriersBindTheirReleasedSerializersAndOriginalIdentity(
      String type, boolean composite, boolean compiled, String mode) throws Exception {
    int versions = mode.startsWith("event-unbounded") ? FlinkPlanCompat.unboundedOverVersion() : 1;
    for (int version = 1; version <= versions; version++)
      new CanonicalBoundedOverPlanTest().assertPlan(
          mode, "BIGINT", false, compiled, "ALWAYS", 0, false, false,
          mode.startsWith("event-unbounded") ? version : FlinkPlanCompat.unboundedOverVersion(),
          type, composite);
  }
}
