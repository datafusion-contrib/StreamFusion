package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventDecimalValueOverJobTest extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "BIGINT:DECIMAL(18,2)");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "BIGINT:DECIMAL(18,2)");
  }
}
