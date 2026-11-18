package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGlobalEventUnboundedRowsJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("rows", false, true, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("rows", false, true, true);
  }

  @Override
  boolean global() {
    return true;
  }
}
