package tech.streamfusion;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end parity tests for the raw native-decode path: a {@code 'format'='raw'} Kafka table
 * decodes each whole message as its single column. {@link NativeParity#assertParity} compares the
 * native decode against Flink's own {@code raw} format for a string column and for fixed-width
 * big-/little-endian numerics. Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers
 * Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeRawDecodeSqlHarnessTest {

  private static final int MESSAGES = 2_000;

  @Test
  void stringMessagesDecodeNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      List<byte[]> messages = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        messages.add(("message-" + i).getBytes(StandardCharsets.UTF_8));
      }
      produce(brokers, "raw-strings", messages);
      NativeParity.assertParity(
          environment(brokers, "raw-strings", "payload STRING", ""), "SELECT * FROM t");
    }
  }

  @Test
  void numericMessagesDecodeNativelyWithFlinkParityInBothEndiannesses() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      for (String endianness : new String[] {"big-endian", "little-endian"}) {
        ByteOrder order =
            "big-endian".equals(endianness) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        List<byte[]> messages = new ArrayList<>(MESSAGES);
        for (int i = 0; i < MESSAGES; i++) {
          messages.add(ByteBuffer.allocate(8).order(order).putLong(i * 1_000_003L - i).array());
        }
        String topic = "raw-bigints-" + endianness;
        produce(brokers, topic, messages);
        NativeParity.assertParity(
            environment(brokers, topic, "payload BIGINT", ", 'raw.endianness' = '" + endianness + "'"),
            "SELECT * FROM t");
      }
    }
  }

  private static Supplier<TableEnvironment> environment(
      String brokers, String topic, String column, String formatOptions) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(
          "CREATE TABLE t ("
              + column
              + ") WITH ('connector' = 'kafka', 'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = '"
              + topic
              + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
              + "'format' = 'raw'"
              + formatOptions
              + ")");
      return tEnv;
    };
  }

  private static void produce(String brokers, String topic, List<byte[]> messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (byte[] message : messages) {
        producer.send(new ProducerRecord<>(topic, 0, null, message));
      }
      producer.flush();
    }
  }
}
