package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventIntKeyOverJobTest extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "INT:BIGINT");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "INT:BIGINT");
  }
}
