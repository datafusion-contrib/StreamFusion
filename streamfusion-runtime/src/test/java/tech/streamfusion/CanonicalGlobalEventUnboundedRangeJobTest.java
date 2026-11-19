package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGlobalEventUnboundedRangeJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("range", false, true, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("range", false, true, true);
  }

  @Override
  boolean global() {
    return true;
  }
}
