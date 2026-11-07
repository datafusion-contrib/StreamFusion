package tech.streamfusion;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalManagedUdfTimestampKeyJobTest extends CanonicalManagedUdfWideKeyJobTest {
  static Stream<Arguments> cases() {
    return CanonicalManagedUdfWideKeyJobTest.cases()
        .filter(
            args ->
                Set.of("TIMESTAMP9", "LOCAL_TIMESTAMP9")
                    .contains(args.get()[0].toString().split("/")[0]));
  }
}
