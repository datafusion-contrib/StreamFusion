package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalEventUnboundedRangeJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("range", false, false, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("range", false, false, true);
  }

  @Override
  boolean global() {
    return false;
  }
}
