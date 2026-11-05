package tech.streamfusion;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalManagedUdfContainerKeyJobTest extends CanonicalManagedUdfWideKeyJobTest {
  static Stream<Arguments> cases() {
    return CanonicalManagedUdfWideKeyJobTest.cases()
        .filter(
            args ->
                Set.of("DECIMAL_ARRAY", "BINARY").contains(args.get()[0].toString().split("/")[0]));
  }
}
