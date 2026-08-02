package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class ConsumerPrefetchTest {

  private static final String PROPERTY = "streamfusion.kafka.prefetch-mb";

  @AfterEach
  void clearProperty() {
    System.clearProperty(PROPERTY);
  }

  private static Map<String, String> tuned() {
    Map<String, String> config = new HashMap<>();
    ConsumerPrefetch.tune(config);
    return config;
  }

  @Test
  void boundsThePrefetchQueueByTheDefaultBudget() {
    Map<String, String> config = tuned();
    assertEquals("2", config.get("fetch.queue.backoff.ms"));
    assertEquals("1000000", config.get("queued.min.messages"));
    assertEquals(String.valueOf(256 * 1024), config.get("queued.max.messages.kbytes"));
  }

  @Test
  void theBudgetKnobRaisesAndLowersTheQueueCeiling() {
    System.setProperty(PROPERTY, "64");
    assertEquals(String.valueOf(64 * 1024), tuned().get("queued.max.messages.kbytes"));
    System.setProperty(PROPERTY, "1024");
    assertEquals(String.valueOf(1024 * 1024), tuned().get("queued.max.messages.kbytes"));
  }

  @Test
  void clampsToLibrdkafkaRange() {
    // librdkafka rejects a whole config whose queued.max.messages.kbytes is outside 1..2097151,
    // so an out-of-range knob value must clamp rather than fail every consumer at startup.
    System.setProperty(PROPERTY, "4096");
    assertEquals("2097151", tuned().get("queued.max.messages.kbytes"));
    System.setProperty(PROPERTY, "0");
    assertEquals("1024", tuned().get("queued.max.messages.kbytes"));
  }

  @Test
  void neverOverridesAnExplicitValue() {
    Map<String, String> config = new HashMap<>(Map.of("queued.min.messages", "5"));
    ConsumerPrefetch.tune(config);
    assertEquals("5", config.get("queued.min.messages"));
  }
}
