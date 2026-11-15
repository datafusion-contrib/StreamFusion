package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedMaterializerDecimalInputKeyJobTest extends CanonicalTypedMaterializerJobTest {
  static Stream<Arguments> cases() {
    return casesForShape("scalar-wide-decimal", true);
  }
}
