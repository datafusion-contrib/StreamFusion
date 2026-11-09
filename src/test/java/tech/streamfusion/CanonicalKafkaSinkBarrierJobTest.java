package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.provider.Arguments;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaSinkBarrierJobTest extends CanonicalKafkaSinkJobTest {
  @Override
  boolean strictBatchBoundaries() {
    return true;
  }

  static Stream<Arguments> cases() {
    return fixtureCases(Stream.of("continue", "no-input", "full-batch"));
  }
}
