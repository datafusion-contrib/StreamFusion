package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUnboundedEventRowsOverExtremaJobTest extends CanonicalUnboundedOverExtremaJobTest {
  static Stream<Arguments> continuedCases() {
    return extremaCases(false, "event", "rows");
  }

  static Stream<Arguments> recoveryCases() {
    return extremaCases(true, "event", "rows");
  }
}
