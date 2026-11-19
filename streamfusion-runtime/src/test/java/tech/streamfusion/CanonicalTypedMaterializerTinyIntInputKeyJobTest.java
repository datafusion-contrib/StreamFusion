package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTypedMaterializerTinyIntInputKeyJobTest extends CanonicalTypedMaterializerJobTest {
  static Stream<Arguments> cases() {
    return casesForShape("scalar-tinyint", true);
  }
}
