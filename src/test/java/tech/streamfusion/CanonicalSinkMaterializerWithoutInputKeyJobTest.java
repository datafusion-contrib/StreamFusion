package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalSinkMaterializerWithoutInputKeyJobTest extends CanonicalSinkMaterializerJobTest {
  static Stream<Arguments> cases() {
    return allCases().filter(args -> (boolean) args.get()[1] == false);
  }
}
