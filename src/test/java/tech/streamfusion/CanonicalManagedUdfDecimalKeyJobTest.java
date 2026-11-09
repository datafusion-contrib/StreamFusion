package tech.streamfusion;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalManagedUdfDecimalKeyJobTest extends CanonicalManagedUdfWideKeyJobTest {
  static Stream<Arguments> cases() {
    return CanonicalManagedUdfWideKeyJobTest.cases()
        .filter(
            args ->
                Set.of("DECIMAL19", "DECIMAL38").contains(args.get()[0].toString().split("/")[0]));
  }
}
