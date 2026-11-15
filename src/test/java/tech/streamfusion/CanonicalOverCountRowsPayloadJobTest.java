package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalOverCountRowsPayloadJobTest extends CanonicalOverCountPayloadJobTest {
  static Stream<Arguments> continuedCases() {
    return payloadCases(false, "rows");
  }

  static Stream<Arguments> recoveryCases() {
    return payloadCases(true, "rows");
  }
}
