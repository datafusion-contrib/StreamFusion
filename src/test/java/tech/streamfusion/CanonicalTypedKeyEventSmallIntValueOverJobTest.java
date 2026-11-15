package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedKeyEventSmallIntValueOverJobTest extends CanonicalTypedKeyEventOverJobTest {
  static Stream<Arguments> continuedCases() {
    return typedCases(false, "BIGINT:SMALLINT");
  }

  static Stream<Arguments> recoveryCases() {
    return typedCases(true, "BIGINT:SMALLINT");
  }
}
