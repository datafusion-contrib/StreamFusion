package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalOrdinaryTemporalSortJobTest extends CanonicalTemporalSortJobTest {
  static Stream<Arguments> continuedCases() {
    return cases(false).filter(args -> (boolean) args.get()[1]);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true).filter(args -> (boolean) args.get()[1]);
  }
}
