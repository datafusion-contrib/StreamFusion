package tech.streamfusion;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaPipelineFailureJobTest extends CanonicalKafkaSinkFailureJobTest {
  @Override
  boolean kafkaInput() {
    return true;
  }
}
