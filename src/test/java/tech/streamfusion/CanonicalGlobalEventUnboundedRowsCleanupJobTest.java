package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGlobalEventUnboundedRowsCleanupJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("rows", true, true, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("rows", true, true, true);
  }

  @Override
  boolean global() {
    return true;
  }
}
