package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalEventUnboundedRowsJobTest extends CanonicalEventUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return eventCases("rows", false, false, false);
  }

  static Stream<Arguments> recoveryCases() {
    return eventCases("rows", false, false, true);
  }

  @Override
  boolean global() {
    return false;
  }
}
