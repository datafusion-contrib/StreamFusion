package tech.streamfusion.planner;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalRetractingExtremaPlanTest {
  static Stream<Arguments> cases() {
    return CanonicalAppendExtremaPlanTest.cases();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void submissionCapturesStructuredHeadersActualMapSerializersAndStockIdentity(
      String mode, String key, boolean compiled, long ttl, String uids) throws Exception {
    new CanonicalGroupArithmeticPlanTest()
        .assertPlan(mode, compiled, ttl, uids, false, key.equals("int"), key.equals("global"));
  }
}
