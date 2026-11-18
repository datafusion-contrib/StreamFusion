package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalOverCountRangePayloadJobTest extends CanonicalOverCountPayloadJobTest {
  static Stream<Arguments> continuedCases() {
    return payloadCases(false, "range");
  }

  static Stream<Arguments> recoveryCases() {
    return payloadCases(true, "range");
  }
}
