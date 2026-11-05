package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.provider.Arguments;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaPartitionDiscoveryJobTest extends CanonicalKafkaSourceJobTest {
  static Stream<Arguments> cases() {
    return Stream.of("discovery-live", "discovery-stopped")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        ordinary ->
                            Stream.of(false, true)
                                .map(first -> Arguments.of(mode, ordinary, first))))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.kafka.discovery.smoke")
                    || args.get()[0].equals("discovery-live")
                        && !(boolean) args.get()[1]
                        && !(boolean) args.get()[2]);
  }
}
