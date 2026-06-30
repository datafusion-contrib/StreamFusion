package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Kafka-sourced Nexmark q0–q2 read through the <em>native rdkafka source</em>: instead of Flink's
 * {@code KafkaSource} consuming the bytes (the {@link NexmarkKafkaBenchmark} decode path), the Rust
 * consumer owns the consume <em>and</em> the decode — librdkafka polls each partition and a background
 * decode thread turns the payloads straight into Arrow, so no {@code RowData} and no Flink Kafka client
 * are on the path. The query, sink, and produced bytes are identical to {@link NexmarkKafkaBenchmark};
 * the only change is the source, so the delta isolates what the native consumer buys over Flink's own
 * Kafka source. This is the full-record decode (no projection pushed into the source yet), so it is a
 * fair head-to-head with Flink's full decode.
 *
 * <p>The native source is opt-in (behind the {@code kafka} cargo feature and the {@code kafkaSource}
 * operator gate), so build the release lib with the feature and run under {@code -Pbench}:
 * {@code SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release --features kafka"
 * -Dtest=NexmarkNativeKafkaSourceBenchmark}. {@code SF_ROWS} overrides the event count.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkNativeKafkaSourceBenchmark {

  private static final int WARMUP = 1;
  private static final int RUNS = 2;
  // Enabling the opt-in native rdkafka source: with it on (and the table a format the source decodes),
  // the planner routes the scan to the native source rather than the shallow Flink-consume decode path.
  private static final String KAFKA_SOURCE_PROPERTY = "streamfusion.operator.kafkaSource.enabled";

  /**
   * Spends the whole JVM lifetime running native-source q0 so a sampler has a stable window over the
   * rdkafka consume + decode-thread stacks. {@code SF_PROFILE=true … -Dprofile.format=json|avro|protobuf
   * -Dprofile.seconds=N}. Skips the Flink baseline.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE", matches = "true")
  void q0NativeProfileLoop() throws Exception {
    String format = System.getProperty("profile.format", "json");
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", format);
      String sinkDdl =
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')";
      String insertSql =
          "INSERT INTO sink SELECT bid.auction, bid.bidder, bid.price, bid.`dateTime`, bid.extra"
              + " FROM src WHERE event_type = 2";
      System.setProperty(KAFKA_SOURCE_PROPERTY, "true");
      long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
      long iterations = 0;
      while (System.currentTimeMillis() < deadline) {
        runOnce(brokers, format, true, sinkDdl, insertSql);
        iterations++;
      }
      System.out.println("[profile] native-source Kafka/" + format + " q0 iterations: " + iterations);
    }
  }

  @Test
  void nexmarkNativeSourceJson() throws Exception {
    runFormat("json");
  }

  @Test
  void nexmarkNativeSourceAvro() throws Exception {
    runFormat("avro");
  }

  @Test
  void nexmarkNativeSourceProtobuf() throws Exception {
    runFormat("protobuf");
  }

  private void runFormat(String format) throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", format);

      compare(
          brokers,
          format,
          "q0 pass-through (project bid fields)",
          false,
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.bidder, bid.price, bid.`dateTime`, bid.extra"
              + " FROM src WHERE event_type = 2");
      compare(
          brokers,
          format,
          "q1 currency conversion (0.908 * price)",
          true,
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.bidder, 0.908 * bid.price, bid.`dateTime`,"
              + " bid.extra FROM src WHERE event_type = 2");
      compare(
          brokers,
          format,
          "q2 filter (MOD(auction, 123) = 0)",
          false,
          "CREATE TABLE sink (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.price FROM src WHERE event_type = 2"
              + " AND MOD(bid.auction, 123) = 0");
    }
  }

  private static void compare(
      String brokers,
      String format,
      String label,
      boolean approximateDecimal,
      String sinkDdl,
      String insertSql)
      throws Exception {
    double flink = bestOf(brokers, format, false, approximateDecimal, sinkDdl, insertSql);
    double nativeRun = bestOf(brokers, format, true, approximateDecimal, sinkDdl, insertSql);
    long rows = NexmarkKafkaBenchmark.ROWS;
    System.out.printf(
        "%n[benchmark] native-source Kafka/%s %s over %,d events (best of %d)%n",
        format.toUpperCase(java.util.Locale.ROOT), label, rows, RUNS);
    System.out.printf("[benchmark]   Flink : %6.3f s  (%,.0f events/s)%n", flink, rows / flink);
    System.out.printf(
        "[benchmark]   Native: %6.3f s  (%,.0f events/s)  %.2fx vs Flink%n",
        nativeRun, rows / nativeRun, flink / nativeRun);
  }

  private static double bestOf(
      String brokers,
      String format,
      boolean useNative,
      boolean approximateDecimal,
      String sinkDdl,
      String insertSql)
      throws Exception {
    String decimalProperty = "streamfusion.expression.decimalArithmetic.approximate";
    String previousDecimal = System.getProperty(decimalProperty);
    String previousSource = System.getProperty(KAFKA_SOURCE_PROPERTY);
    if (useNative) {
      System.setProperty(KAFKA_SOURCE_PROPERTY, "true");
      if (approximateDecimal) {
        System.setProperty(decimalProperty, "true");
      }
    }
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < WARMUP + RUNS; run++) {
        double seconds = runOnce(brokers, format, useNative, sinkDdl, insertSql);
        if (run >= WARMUP) {
          best = Math.min(best, seconds);
        }
      }
      return best;
    } finally {
      restore(decimalProperty, previousDecimal);
      restore(KAFKA_SOURCE_PROPERTY, previousSource);
    }
  }

  private static void restore(String key, String previous) {
    if (previous == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, previous);
    }
  }

  private static double runOnce(
      String brokers, String format, boolean useNative, String sinkDdl, String insertSql)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ") WITH ('connector' = 'kafka', 'topic' = 'nexmark', 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'properties.group.id' = 'nexmark', 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset', 'format' = '"
            + format
            + "'"
            + ("protobuf".equals(format)
                ? ", 'protobuf.message-class-name' = 'io.github.jordepic.streamfusion.proto.NexmarkEvent'"
                : "")
            + ")");
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(sinkDdl);
    long start = System.nanoTime();
    tEnv.executeSql(insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException(
          "native source did not engage; comparison is moot. " + scan.fallbackReasons());
    }
    return seconds;
  }
}
