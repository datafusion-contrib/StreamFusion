package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.format.json.NativeJsonFormat;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaExtensionTest {

  @Test
  void loadsTheKafkaJniFacadeFromTheDevelopmentLibrary() {
    assertTrue(NativeKafka.featureBuilt());
  }

  /**
   * The format library must export a real driver init for the connector's attach handshake; a
   * refused or absent init falls back to the JVM-mediated decode by construction, which the fused
   * ingest parity tests would surface as a plain (slower) pass.
   */
  @Test
  void formatExportsADriverInit() {
    assertNotEquals(0, NativeJsonFormat.driverInitAddress());
  }
}
