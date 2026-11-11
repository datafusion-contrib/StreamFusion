package tech.streamfusion.planner.stream;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGlobalGroupArithmeticPlanTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticPlanTest.cases()
        .filter(test -> !(boolean) test.get()[5])
        .map(
            test ->
                Arguments.of(
                    test.get()[0], test.get()[1], test.get()[2], test.get()[3], test.get()[4]));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalGlobalSubmissionsKeepStockSingletonIdentityAndState(
      String mode, boolean compiled, long ttl, String uids, boolean append) throws Exception {
    new CanonicalGroupArithmeticPlanTest()
        .assertPlan(mode, compiled, ttl, uids, append, false, true);
  }
}
