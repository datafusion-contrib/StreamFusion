package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUnboundedProcessingOverExtremaJobTest extends CanonicalUnboundedOverExtremaJobTest {
  static Stream<Arguments> continuedCases() {
    return extremaCases(false, "proc", "rows", "range");
  }

  static Stream<Arguments> recoveryCases() {
    return extremaCases(true, "proc", "rows", "range");
  }
}
