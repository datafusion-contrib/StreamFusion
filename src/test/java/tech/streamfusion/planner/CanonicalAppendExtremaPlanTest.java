package tech.streamfusion.planner;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalAppendExtremaPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of("bigint", "int", "global")
                    .flatMap(
                        key ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of(0L, 1000L)
                                            .flatMap(
                                                ttl ->
                                                    Stream.of("DISABLED", "ALWAYS")
                                                        .map(
                                                            uids ->
                                                                Arguments.of(
                                                                    mode, key, compiled, ttl,
                                                                    uids))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionBindsActualNullableAccumulatorsAndStockIdentity(
      String mode, String key, boolean compiled, long ttl, String uids) throws Exception {
    new CanonicalGroupArithmeticPlanTest()
        .assertPlan(mode, compiled, ttl, uids, true, key.equals("int"), key.equals("global"));
  }
}
