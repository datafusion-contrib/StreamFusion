package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGlobalEventUnboundedRangeCleanupJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("range", true, true, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("range", true, true, true);
  }

  @Override
  boolean global() {
    return true;
  }
}
