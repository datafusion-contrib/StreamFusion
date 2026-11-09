package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalEventUnboundedRangeCleanupJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("range", true, false, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("range", true, false, true);
  }

  @Override
  boolean global() {
    return false;
  }
}
