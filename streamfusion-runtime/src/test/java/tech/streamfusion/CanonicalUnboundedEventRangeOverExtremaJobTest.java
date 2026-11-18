package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUnboundedEventRangeOverExtremaJobTest extends CanonicalUnboundedOverExtremaJobTest {
  static Stream<Arguments> continuedCases() {
    return extremaCases(false, "event", "range");
  }

  static Stream<Arguments> recoveryCases() {
    return extremaCases(true, "event", "range");
  }
}
