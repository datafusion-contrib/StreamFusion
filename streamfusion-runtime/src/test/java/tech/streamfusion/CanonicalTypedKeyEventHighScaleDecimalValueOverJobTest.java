package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventHighScaleDecimalValueOverJobTest
    extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "BIGINT:DECIMAL(38,20)");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "BIGINT:DECIMAL(38,20)");
  }
}
