package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.provider.Arguments;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaSinkFailureJobTest extends CanonicalKafkaSinkJobTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            ordinary ->
                Stream.of(false, true)
                    .flatMap(
                        first ->
                            Stream.of("incrementing", "pooling")
                                .map(
                                    naming ->
                                        Arguments.of("task-failure", ordinary, first, naming))));
  }
}
