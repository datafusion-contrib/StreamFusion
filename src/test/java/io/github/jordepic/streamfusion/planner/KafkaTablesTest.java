package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class KafkaTablesTest {

  /**
   * Partition discovery must run on the same cadence as Flink's own table source: the factory maps
   * {@code scan.topic-partition-discovery.interval} (default 5 minutes, 0 disables) onto the
   * enumerator's property unconditionally, overriding even an explicit {@code properties.*} value.
   * Diverging here changes what data a job reads — a disabled-discovery table must not pick up
   * partitions Flink would ignore.
   */
  @Test
  void discoveryIntervalMirrorsFlinkTableFactory() {
    assertEquals("300000", discoveryInterval(Map.of()));
    assertEquals(
        "30000", discoveryInterval(Map.of("scan.topic-partition-discovery.interval", "30 s")));
    assertEquals("0", discoveryInterval(Map.of("scan.topic-partition-discovery.interval", "0")));
    assertEquals(
        "300000",
        discoveryInterval(Map.of("properties.partition.discovery.interval.ms", "1000")));
  }

  @Test
  void groupIdentityAndOffsetResetFollowFlinkSourceBuilder() {
    Properties raw = KafkaTables.consumerProperties(Map.of());
    assertFalse(raw.containsKey("group.id"), "a table without a group must remain groupless");

    Properties earliest =
        KafkaTables.configuredSourceProperties(
            Map.of("properties.auto.offset.reset", "latest"), OffsetsInitializer.earliest());
    assertEquals("earliest", earliest.getProperty("auto.offset.reset"));
    assertEquals("false", earliest.getProperty("commit.offsets.on.checkpoint"));
    org.junit.jupiter.api.Assertions.assertTrue(
        earliest.getProperty("client.id.prefix").startsWith("KafkaSource-"));

    Properties grouped =
        KafkaTables.configuredSourceProperties(
            Map.of("properties.group.id", "orders"), OffsetsInitializer.earliest());
    assertEquals("orders", grouped.getProperty("client.id.prefix"));

    assertEquals(
        OffsetResetStrategy.NONE,
        KafkaTables.mapStartupMode(Map.of()).getAutoOffsetResetStrategy());
    assertEquals(
        OffsetResetStrategy.EARLIEST,
        KafkaTables.mapStartupMode(
                Map.of(
                    "scan.startup.mode",
                    "group-offsets",
                    "properties.auto.offset.reset",
                    "earliest"))
            .getAutoOffsetResetStrategy());
  }

  private static String discoveryInterval(Map<String, String> options) {
    Properties props = KafkaTables.consumerProperties(options);
    return props.getProperty("partition.discovery.interval.ms");
  }
}
