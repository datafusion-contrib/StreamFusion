package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Kafka → columnar ladder: how far into Rust the source-side work moves, measured against stock
 * Flink on the same q0/q1/q2 over the same produced bytes. Four ways to feed the columnar query engine,
 * each a rung more native than the last:
 *
 * <ol>
 *   <li><b>Flink</b> — stock Flink, no native island (RowData straight to the sink). The baseline.
 *   <li><b>JVM transpose</b> — Flink consumes <em>and</em> decodes to RowData with its own format, then a
 *       JVM {@code RowData → Arrow} transpose feeds the native calc (the projection pruned into the
 *       transpose). The row→columnar boundary is paid on the JVM.
 *   <li><b>Rust transpose, JVM poll</b> — Flink's {@code KafkaSource} polls raw bytes, a native operator
 *       decodes the bytes straight to Arrow (the transpose is in Rust; the projection is pushed into the
 *       decode). The shallow decode path.
 *   <li><b>Rust poll + Rust transpose</b> — the native rdkafka source: Rust owns the consume and the
 *       decode-to-Arrow (projection pushed into the source). No Flink Kafka client, no RowData.
 * </ol>
 *
 * <p>Each rung removes one layer of JVM/row work. Opt-in: {@code SF_BENCHMARK=true mvn test -Pbench
 * -Dnative.cargo.args="build --release --features kafka" -Dtest=NexmarkKafkaLadderBenchmark} (Docker for
 * Testcontainers Kafka; the native source needs the {@code kafka} feature). {@code SF_ROWS} overrides the
 * event count (default 1,000,000). {@code SF_LADDER_FORMATS} overrides the formats (default all three).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkKafkaLadderBenchmark {

  private static final int WARMUP = 1;
  private static final int RUNS = 2;

  /** A rung of the ladder: the planner config (system properties) that selects how Arrow is produced. */
  private enum Rung {
    FLINK("Flink", Map.of("streamfusion.native.enabled", "false")),
    JVM_TRANSPOSE(
        "JVM transpose",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaDecode.enabled", "false",
            "streamfusion.operator.kafkaSource.enabled", "false")),
    RUST_DECODE(
        "Rust transpose, JVM poll",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaDecode.enabled", "true",
            "streamfusion.operator.kafkaSource.enabled", "false")),
    RUST_SOURCE(
        "Rust poll + Rust transpose",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaSource.enabled", "true"));

    final String label;
    final Map<String, String> properties;

    Rung(String label, Map<String, String> properties) {
      this.label = label;
      this.properties = properties;
    }
  }

  private static final class Query {
    final String label;
    final boolean approximateDecimal;
    final String sinkDdl;
    final String insertSql;

    Query(String label, boolean approximateDecimal, String sinkDdl, String insertSql) {
      this.label = label;
      this.approximateDecimal = approximateDecimal;
      this.sinkDdl = sinkDdl;
      this.insertSql = insertSql;
    }
  }

  private static final Query[] QUERIES = {
    new Query(
        "q0 pass-through",
        false,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` BIGINT,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT bid.auction, bid.bidder, bid.price, bid.`dateTime`, bid.extra"
            + " FROM src WHERE event_type = 2"),
    new Query(
        "q1 currency",
        true,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` BIGINT,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT bid.auction, bid.bidder, 0.908 * bid.price, bid.`dateTime`,"
            + " bid.extra FROM src WHERE event_type = 2"),
    new Query(
        "q2 filter",
        false,
        "CREATE TABLE sink (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT bid.auction, bid.price FROM src WHERE event_type = 2"
            + " AND MOD(bid.auction, 123) = 0"),
  };

  @Test
  void ladder() throws Exception {
    String formatsEnv = System.getenv("SF_LADDER_FORMATS");
    String[] formats = formatsEnv != null ? formatsEnv.split(",") : new String[] {"json", "avro", "protobuf"};
    long rows = NexmarkKafkaBenchmark.ROWS;
    for (String format : formats) {
      try (KafkaContainer kafka =
          new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
        kafka.start();
        String brokers = kafka.getBootstrapServers();
        NexmarkKafkaBenchmark.produce(brokers, "nexmark", format);
        for (Query query : QUERIES) {
          Map<Rung, Double> best = new LinkedHashMap<>();
          for (Rung rung : Rung.values()) {
            best.put(rung, bestOf(brokers, format, rung, query));
          }
          report(format, query, rows, best);
        }
      }
    }
  }

  private static void report(String format, Query query, long rows, Map<Rung, Double> best) {
    double flink = best.get(Rung.FLINK);
    System.out.printf(
        "%n[ladder] Kafka/%s %s over %,d events (best of %d)%n",
        format.toUpperCase(java.util.Locale.ROOT), query.label, rows, RUNS);
    for (Rung rung : Rung.values()) {
      double s = best.get(rung);
      String speedup = rung == Rung.FLINK ? "baseline" : String.format("%.2fx vs Flink", flink / s);
      System.out.printf(
          "[ladder]   %-28s %6.3f s  (%,.0f ev/s)  %s%n", rung.label, s, rows / s, speedup);
    }
  }

  private static double bestOf(String brokers, String format, Rung rung, Query query) throws Exception {
    Map<String, String> previous = new LinkedHashMap<>();
    String decimalProperty = "streamfusion.expression.decimalArithmetic.approximate";
    boolean nativeRun = !"false".equals(rung.properties.get("streamfusion.native.enabled"));
    Map<String, String> props = new LinkedHashMap<>(rung.properties);
    if (nativeRun && query.approximateDecimal) {
      props.put(decimalProperty, "true");
    }
    props.forEach((k, v) -> previous.put(k, System.getProperty(k)));
    props.forEach(System::setProperty);
    try {
      double bestSeconds = Double.MAX_VALUE;
      for (int run = 0; run < WARMUP + RUNS; run++) {
        double seconds = runOnce(brokers, format, nativeRun, query);
        if (run >= WARMUP) {
          bestSeconds = Math.min(bestSeconds, seconds);
        }
      }
      return bestSeconds;
    } finally {
      previous.forEach(
          (k, v) -> {
            if (v == null) {
              System.clearProperty(k);
            } else {
              System.setProperty(k, v);
            }
          });
    }
  }

  private static double runOnce(String brokers, String format, boolean nativeRun, Query query)
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
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(query.sinkDdl);
    long start = System.nanoTime();
    tEnv.executeSql(query.insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun && scan.substitutions() == 0) {
      throw new IllegalStateException(
          "native island did not engage; comparison is moot. " + scan.fallbackReasons());
    }
    return seconds;
  }
}
