package tech.streamfusion;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalManagedUdfStructuredKeyJobTest extends CanonicalManagedUdfWideKeyJobTest {
  static Stream<Arguments> cases() {
    return CanonicalManagedUdfWideKeyJobTest.cases()
        .filter(
            args ->
                Set.of("COMPOSITE", "NESTED_ROW").contains(args.get()[0].toString().split("/")[0]));
  }
}
