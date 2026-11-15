package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGroupTypedNoInputJobTest extends CanonicalGroupTypedJobBase {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases().filter(test -> test.get()[0].equals("no-input"));
  }
}
