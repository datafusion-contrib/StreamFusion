package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaExtensionTest {

  @Test
  void loadsTheKafkaJniFacadeFromTheDevelopmentLibrary() {
    assertTrue(NativeKafka.featureBuilt());
  }
}
