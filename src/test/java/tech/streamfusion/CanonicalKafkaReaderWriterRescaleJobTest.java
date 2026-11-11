package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.provider.Arguments;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaReaderWriterRescaleJobTest extends CanonicalKafkaSinkJobTest {
  static Stream<Arguments> cases() {
    return fixtureCases(Stream.of("rescale-uids"));
  }

  @Override
  boolean kafkaInput() {
    return true;
  }

  @Override
  boolean rescaleReaders() {
    return true;
  }
}
