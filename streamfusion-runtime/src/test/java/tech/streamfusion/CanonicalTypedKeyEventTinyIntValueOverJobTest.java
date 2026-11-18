package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventTinyIntValueOverJobTest extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "BIGINT:TINYINT");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "BIGINT:TINYINT");
  }
}
