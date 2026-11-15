package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalEventUnboundedRowsCleanupJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("rows", true, false, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("rows", true, false, true);
  }

  @Override
  boolean global() {
    return false;
  }
}
