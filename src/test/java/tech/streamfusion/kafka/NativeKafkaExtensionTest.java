package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.FormatCodes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaExtensionTest {

  @Test
  void loadsTheKafkaSerializationFacadeFromTheDevelopmentLibrary() {
    assertTrue(NativeKafka.isLoaded());
    assertTrue(NativeKafka.encodeFormatSupported(FormatCodes.JSON));
  }
}
