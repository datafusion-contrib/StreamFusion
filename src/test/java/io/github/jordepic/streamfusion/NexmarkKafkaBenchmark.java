package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
 * Kafka-sourced Nexmark q0–q2: the wide event row arrives as JSON on a topic, read through a {@code
 * 'format'='json'} table. Native decode (Flink consumes raw bytes, a native operator decodes them to
 * Arrow, with the query's projection pushed into the decode) is compared to Flink's own {@code json}
 * format, over the same bytes and the same query. Flink does not push projection into the Kafka scan,
 * so it decodes the whole record; the native decoder builds only the columns/nested fields the query
 * reads — the win this benchmark isolates. Complements {@link NexmarkBenchmark} (the generator source).
 *
 * <p>Opt-in: {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka). {@code dateTime}/{@code
 * expires} are epoch-millis BIGINT here (the decode cost is a long either way; avoids JSON timestamp
 * parsing noise). {@code SF_ROWS} overrides the event count (default 1,000,000).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkKafkaBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 1_000_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;
  private static final int BLOCK = 50;
  private static final String[] STATES = {"OR", "ID", "CA", "WA", "NY", "TX"};

  private static final String SCHEMA =
      "event_type INT,"
          + " person ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING,"
          + " state STRING, `dateTime` BIGINT, extra STRING>,"
          + " auction ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
          + " reserve BIGINT, `dateTime` BIGINT, expires BIGINT, seller BIGINT, category BIGINT,"
          + " extra STRING>,"
          + " bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
          + " `dateTime` BIGINT, extra STRING>,"
          + " `dateTime` BIGINT";

  @Test
  void nexmarkKafkaJson() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, "nexmark");

      compare(
          brokers,
          "q0 pass-through (project bid fields)",
          false,
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.bidder, bid.price, bid.`dateTime`, bid.extra"
              + " FROM src WHERE event_type = 2");
      compare(
          brokers,
          "q1 currency conversion (0.908 * price)",
          true,
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.bidder, 0.908 * bid.price, bid.`dateTime`,"
              + " bid.extra FROM src WHERE event_type = 2");
      compare(
          brokers,
          "q2 filter (MOD(auction, 123) = 0)",
          false,
          "CREATE TABLE sink (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.price FROM src WHERE event_type = 2"
              + " AND MOD(bid.auction, 123) = 0");
    }
  }

  private static void compare(
      String brokers, String label, boolean approximateDecimal, String sinkDdl, String insertSql)
      throws Exception {
    double flink = bestOf(brokers, false, approximateDecimal, sinkDdl, insertSql);
    double nativeRun = bestOf(brokers, true, approximateDecimal, sinkDdl, insertSql);
    System.out.printf("%n[benchmark] Kafka/JSON %s over %,d events (best of %d)%n", label, ROWS, RUNS);
    System.out.printf("[benchmark]   Flink : %6.3f s  (%,.0f events/s)%n", flink, ROWS / flink);
    System.out.printf(
        "[benchmark]   Native: %6.3f s  (%,.0f events/s)  %.2fx vs Flink%n",
        nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static double bestOf(
      String brokers, boolean useNative, boolean approximateDecimal, String sinkDdl, String insertSql)
      throws Exception {
    String property = "streamfusion.expression.decimalArithmetic.approximate";
    String previous = System.getProperty(property);
    if (useNative && approximateDecimal) {
      System.setProperty(property, "true");
    }
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < WARMUP + RUNS; run++) {
        double seconds = runOnce(brokers, useNative, sinkDdl, insertSql);
        if (run >= WARMUP) {
          best = Math.min(best, seconds);
        }
      }
      return best;
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  private static double runOnce(
      String brokers, boolean useNative, String sinkDdl, String insertSql) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + SCHEMA
            + ") WITH ('connector' = 'kafka', 'topic' = 'nexmark', 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'properties.group.id' = 'nexmark', 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset', 'format' = 'json')");
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(sinkDdl);
    long start = System.nanoTime();
    tEnv.executeSql(insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException(
          "native decode did not engage; comparison is moot. " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static void produce(String brokers, String topic) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>();
      for (long i = 0; i < ROWS; i++) {
        producer.send(new ProducerRecord<>(topic, 0, null, event(i).getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
  }

  /** One wide event as JSON, the inactive structs null — the Nexmark person/auction/bid mix. */
  private static String event(long i) {
    long block = i / BLOCK;
    int pos = (int) (i % BLOCK);
    long ts = block * 1000L + pos * 10L;
    if (pos == 0) {
      return String.format(
          "{\"event_type\":0,\"person\":{\"id\":%d,\"name\":\"p-%d\",\"emailAddress\":\"e-%d\","
              + "\"creditCard\":\"1234\",\"city\":\"c-%d\",\"state\":\"%s\",\"dateTime\":%d,"
              + "\"extra\":\"x\"},\"auction\":null,\"bid\":null,\"dateTime\":%d}",
          block, block, block, block % 1000, STATES[(int) (block % STATES.length)], ts, ts);
    }
    if (pos <= 3) {
      long auctionId = block * 3 + (pos - 1);
      return String.format(
          "{\"event_type\":1,\"person\":null,\"auction\":{\"id\":%d,\"itemName\":\"i-%d\","
              + "\"description\":\"d-%d\",\"initialBid\":10,\"reserve\":50,\"dateTime\":%d,"
              + "\"expires\":%d,\"seller\":%d,\"category\":%d,\"extra\":\"x\"},\"bid\":null,"
              + "\"dateTime\":%d}",
          auctionId, auctionId, auctionId, ts, ts + 20000, block, block % 100, ts);
    }
    long auctionId = block * 3 + (pos % 3);
    return String.format(
        "{\"event_type\":2,\"person\":null,\"auction\":null,\"bid\":{\"auction\":%d,\"bidder\":%d,"
            + "\"price\":%d,\"channel\":\"ch-%d\",\"url\":\"https://n.test/%d\",\"dateTime\":%d,"
            + "\"extra\":\"x\"},\"dateTime\":%d}",
        auctionId, pos, (i % 1000) + 1, pos % 8, auctionId, ts, ts);
  }
}
