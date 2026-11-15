package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventIntValueOverJobTest extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "BIGINT:INT");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "BIGINT:INT");
  }
}
