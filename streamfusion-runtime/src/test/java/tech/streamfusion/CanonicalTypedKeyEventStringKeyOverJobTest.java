package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventStringKeyOverJobTest extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "STRING:BIGINT");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "STRING:BIGINT");
  }
}
