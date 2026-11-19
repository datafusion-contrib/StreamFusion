package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGlobalGroupRescaleRegressionTest extends CanonicalGlobalGroupArithmeticJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases().filter(args -> args.get()[0].equals("rescale"));
  }
}
