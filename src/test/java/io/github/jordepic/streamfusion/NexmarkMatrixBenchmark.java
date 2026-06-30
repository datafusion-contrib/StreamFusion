package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The full Nexmark matrix: every query StreamFusion currently accelerates end-to-end, each run against
 * stock Flink and against native execution from every source it can be fed by — the generator (rowwise
 * RowData) and Kafka json/avro/protobuf, the Kafka formats climbing the source→columnar ladder (JVM
 * transpose, Rust decode with a JVM poll, fully native Rust poll+decode). One table, ten native cells
 * per query, each a speedup over the Flink baseline for that same source.
 *
 * <p>The query set is the accelerating subset reported by {@link NexmarkExplainTest}: q0–q4, q7–q12,
 * q15–q20, q22 (q1/q14-style decimal needs the approximate-decimal flag; q14's non-builtin UDF, q5/q6's
 * unsupported window shape, q13's temporal join, q21's REGEXP_EXTRACT and q23's parse all keep them out).
 * Each query runs over the same logical {@code person}/{@code auction}/{@code bid} views the published
 * Nexmark SQL uses, off a watermarked event-time {@code dateTime}; the only thing that changes between
 * cells is how those rows are produced and transposed into the columnar island. The perimeter transposes
 * (source and sink) stay in the measured path — the steelman per CLAUDE.md.
 *
 * <p>Opt-in (millions of rows, Docker for Testcontainers Kafka, the {@code kafka} cargo feature for the
 * native source): {@code SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release
 * --features kafka" -Dtest=NexmarkMatrixBenchmark}. {@code SF_ROWS} overrides the event count (default
 * 500,000), {@code SF_MATRIX_QUERIES} a comma-separated query subset (e.g. {@code q0,q7,q15}), {@code
 * SF_LADDER_FORMATS} the Kafka formats (default {@code json,avro,protobuf}), {@code SF_MATRIX_GENERATOR}
 * ({@code false} to skip the generator column), {@code SF_MATRIX_KAFKA} ({@code false} to skip Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkMatrixBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 500_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;

  /** A rung of the Kafka source→columnar ladder (mirrors {@link NexmarkKafkaLadderBenchmark}). */
  private enum Rung {
    FLINK("Flink", Map.of("streamfusion.native.enabled", "false")),
    JVM_TRANSPOSE(
        "JVM transpose",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaDecode.enabled", "false",
            "streamfusion.operator.kafkaSource.enabled", "false")),
    RUST_DECODE(
        "Rust decode (JVM poll)",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaDecode.enabled", "true",
            "streamfusion.operator.kafkaSource.enabled", "false")),
    RUST_SOURCE(
        "Rust poll + decode",
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
    final String extraView; // an extra view created before the insert (q12's proctime view); else null
    final String sinkDdl; // %TS% substituted with the harness's event-time type
    final String insertSql;

    Query(String label, boolean approximateDecimal, String extraView, String sinkDdl, String insertSql) {
      this.label = label;
      this.approximateDecimal = approximateDecimal;
      this.extraView = extraView;
      this.sinkDdl = sinkDdl;
      this.insertSql = insertSql;
    }
  }

  private static final Query[] ALL_QUERIES = {
    new Query(
        "q0",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, `dateTime`, extra FROM bid"),
    new Query(
        "q1",
        true,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, 0.908 * price AS price, `dateTime`, extra FROM bid"),
    new Query(
        "q2",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0"),
    new Query(
        "q3",
        false,
        null,
        "CREATE TABLE sink (name STRING, city STRING, state STRING, id BIGINT) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT P.name, P.city, P.state, A.id FROM auction AS A INNER JOIN"
            + " person AS P ON A.seller = P.id WHERE A.category = 10 AND (P.state = 'OR' OR P.state"
            + " = 'ID' OR P.state = 'CA')"),
    new Query(
        "q4",
        false,
        null,
        "CREATE TABLE sink (id BIGINT, final BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT Q.category, AVG(Q.final) FROM (SELECT MAX(B.price) AS final,"
            + " A.category FROM auction A, bid B WHERE A.id = B.auction AND B.`dateTime` BETWEEN"
            + " A.`dateTime` AND A.expires GROUP BY A.id, A.category) Q GROUP BY Q.category"),
    new Query(
        "q7",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, price BIGINT, bidder BIGINT, `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.auction, B.price, B.bidder, B.`dateTime`, B.extra FROM bid B JOIN"
            + " (SELECT MAX(price) AS maxprice, window_end AS `dateTime` FROM"
            + " TABLE(TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))"
            + " GROUP BY window_start, window_end) B1 ON B.price = B1.maxprice"
            + " WHERE B.`dateTime` BETWEEN B1.`dateTime` - INTERVAL '10' SECOND AND B1.`dateTime`"),
    new Query(
        "q8",
        false,
        null,
        "CREATE TABLE sink (id BIGINT, name STRING, stime %WTS%) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT P.id, P.name, P.starttime FROM (SELECT id, name,"
            + " window_start AS starttime, window_end AS endtime FROM"
            + " TABLE(TUMBLE(TABLE person, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))"
            + " GROUP BY id, name, window_start, window_end) P JOIN (SELECT seller,"
            + " window_start AS starttime, window_end AS endtime FROM"
            + " TABLE(TUMBLE(TABLE auction, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))"
            + " GROUP BY seller, window_start, window_end) A"
            + " ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime"),
    new Query(
        "q9",
        false,
        null,
        "CREATE TABLE sink (id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
            + " reserve BIGINT, `dateTime` %TS%, expires %TS%, seller BIGINT, category BIGINT,"
            + " extra STRING, auction BIGINT, bidder BIGINT, price BIGINT, bid_dateTime %TS%,"
            + " bid_extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT id, itemName, description, initialBid, reserve, `dateTime`, expires,"
            + " seller, category, extra, auction, bidder, price, bid_dateTime, bid_extra FROM (SELECT"
            + " A.*, B.auction, B.bidder, B.price, B.`dateTime` AS bid_dateTime, B.extra AS bid_extra,"
            + " ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.`dateTime` ASC) AS rownum"
            + " FROM auction A, bid B WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND"
            + " A.expires) WHERE rownum <= 1"),
    new Query(
        "q10",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` %TS%,"
            + " extra STRING, dt STRING, hm STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, `dateTime`, extra,"
            + " DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm') FROM bid"),
    new Query(
        "q11",
        false,
        null,
        "CREATE TABLE sink (bidder BIGINT, bid_count BIGINT, starttime %WTS%, endtime %WTS%) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.bidder, count(*) AS bid_count,"
            + " SESSION_START(B.`dateTime`, INTERVAL '10' SECOND) AS starttime,"
            + " SESSION_END(B.`dateTime`, INTERVAL '10' SECOND) AS endtime FROM bid B"
            + " GROUP BY B.bidder, SESSION(B.`dateTime`, INTERVAL '10' SECOND)"),
    new Query(
        "q12",
        false,
        "CREATE TEMPORARY VIEW bid_proc AS SELECT *, PROCTIME() AS p_time FROM bid",
        "CREATE TABLE sink (bidder BIGINT, bid_count BIGINT, starttime %WTS%, endtime %WTS%) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT bidder, count(*) AS bid_count, window_start AS starttime,"
            + " window_end AS endtime FROM TABLE(TUMBLE(TABLE bid_proc, DESCRIPTOR(p_time),"
            + " INTERVAL '10' SECOND)) GROUP BY bidder, window_start, window_end"),
    new Query(
        "q15",
        false,
        null,
        "CREATE TABLE sink (`day` STRING, total_bids BIGINT, rank1_bids BIGINT, rank2_bids BIGINT,"
            + " rank3_bids BIGINT, total_bidders BIGINT, rank1_bidders BIGINT, rank2_bidders BIGINT,"
            + " rank3_bidders BIGINT, total_auctions BIGINT, rank1_auctions BIGINT,"
            + " rank2_auctions BIGINT, rank3_auctions BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') AS `day`, count(*) AS"
            + " total_bids, count(*) filter (where price < 10000) AS rank1_bids, count(*) filter"
            + " (where price >= 10000 and price < 1000000) AS rank2_bids, count(*) filter (where price"
            + " >= 1000000) AS rank3_bids, count(distinct bidder) AS total_bidders, count(distinct"
            + " bidder) filter (where price < 10000) AS rank1_bidders, count(distinct bidder) filter"
            + " (where price >= 10000 and price < 1000000) AS rank2_bidders, count(distinct bidder)"
            + " filter (where price >= 1000000) AS rank3_bidders, count(distinct auction) AS"
            + " total_auctions, count(distinct auction) filter (where price < 10000) AS rank1_auctions,"
            + " count(distinct auction) filter (where price >= 10000 and price < 1000000) AS"
            + " rank2_auctions, count(distinct auction) filter (where price >= 1000000) AS"
            + " rank3_auctions FROM bid GROUP BY DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')"),
    new Query(
        "q16",
        false,
        null,
        "CREATE TABLE sink (channel STRING, `day` STRING, `minute` STRING, total_bids BIGINT,"
            + " rank1_bids BIGINT, rank2_bids BIGINT, rank3_bids BIGINT, total_bidders BIGINT,"
            + " rank1_bidders BIGINT, rank2_bidders BIGINT, rank3_bidders BIGINT, total_auctions"
            + " BIGINT, rank1_auctions BIGINT, rank2_auctions BIGINT, rank3_auctions BIGINT) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT channel, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') AS `day`,"
            + " max(DATE_FORMAT(`dateTime`, 'HH:mm')) AS `minute`, count(*) AS total_bids, count(*)"
            + " filter (where price < 10000) AS rank1_bids, count(*) filter (where price >= 10000 and"
            + " price < 1000000) AS rank2_bids, count(*) filter (where price >= 1000000) AS rank3_bids,"
            + " count(distinct bidder) AS total_bidders, count(distinct bidder) filter (where price <"
            + " 10000) AS rank1_bidders, count(distinct bidder) filter (where price >= 10000 and price"
            + " < 1000000) AS rank2_bidders, count(distinct bidder) filter (where price >= 1000000) AS"
            + " rank3_bidders, count(distinct auction) AS total_auctions, count(distinct auction)"
            + " filter (where price < 10000) AS rank1_auctions, count(distinct auction) filter (where"
            + " price >= 10000 and price < 1000000) AS rank2_auctions, count(distinct auction) filter"
            + " (where price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel,"
            + " DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')"),
    new Query(
        "q17",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, `day` STRING, total_bids BIGINT, rank1_bids BIGINT,"
            + " rank2_bids BIGINT, rank3_bids BIGINT, min_price BIGINT, max_price BIGINT, avg_price"
            + " BIGINT, sum_price BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') AS `day`, count(*) AS"
            + " total_bids, count(*) filter (where price < 10000) AS rank1_bids, count(*) filter (where"
            + " price >= 10000 and price < 1000000) AS rank2_bids, count(*) filter (where price >="
            + " 1000000) AS rank3_bids, min(price) AS min_price, max(price) AS max_price, avg(price) AS"
            + " avg_price, sum(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT(`dateTime`,"
            + " 'yyyy-MM-dd')"),
    new Query(
        "q18",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + " `dateTime` %TS%, extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, url, `dateTime`, extra FROM (SELECT"
            + " *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY `dateTime` DESC) AS"
            + " rank_number FROM bid) WHERE rank_number <= 1"),
    new Query(
        "q19",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + " `dateTime` %TS%, extra STRING, rank_number BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY"
            + " price DESC) AS rank_number FROM bid) WHERE rank_number <= 10"),
    new Query(
        "q20",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + " bid_dateTime %TS%, bid_extra STRING, itemName STRING, description STRING, initialBid"
            + " BIGINT, reserve BIGINT, auction_dateTime %TS%, expires %TS%, seller BIGINT, category"
            + " BIGINT, auction_extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, url, B.`dateTime`, B.extra, itemName,"
            + " description, initialBid, reserve, A.`dateTime`, expires, seller, category, A.extra FROM"
            + " bid AS B INNER JOIN auction AS A ON B.auction = A.id WHERE A.category = 10"),
    new Query(
        "q22",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, dir1 STRING,"
            + " dir2 STRING, dir3 STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1,"
            + " SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM bid"),
  };

  // DATE_FORMAT is native only over a plain TIMESTAMP (LTZ formatting is session-zone dependent), but
  // the Kafka source's event-time column is TIMESTAMP_LTZ (the epoch-millis decode + a TO_TIMESTAMP_LTZ
  // rowtime). These queries therefore run on the generator (plain TIMESTAMP) only; on Kafka they would
  // partially fall back, so they are skipped rather than reported as a half-native comparison.
  private static final Set<String> GENERATOR_ONLY = Set.of("q10", "q15", "q16", "q17");

  @Test
  void matrix() throws Exception {
    Query[] queries = selectQueries();
    boolean runGenerator = !"false".equals(System.getenv("SF_MATRIX_GENERATOR"));
    boolean runKafka = !"false".equals(System.getenv("SF_MATRIX_KAFKA"));
    String formatsEnv = System.getenv("SF_LADDER_FORMATS");
    String[] formats =
        formatsEnv != null ? formatsEnv.split(",") : new String[] {"json", "avro", "protobuf"};

    // result[label] -> ordered cells (rendered at the end as one table).
    Map<String, List<String>> report = new LinkedHashMap<>();
    for (Query q : queries) {
      report.put(q.label, new ArrayList<>());
    }

    if (runGenerator) {
      for (Query q : queries) {
        double flink = generatorBest(q, false);
        double nativeRun = generatorBest(q, true);
        report.get(q.label).add(cell("generator", flink, nativeRun));
      }
    }

    if (runKafka) {
      for (String format : formats) {
        try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
          kafka.start();
          String brokers = kafka.getBootstrapServers();
          NexmarkKafkaBenchmark.produce(brokers, "nexmark", format, ROWS);
          for (Query q : queries) {
            if (GENERATOR_ONLY.contains(q.label)) {
              report.get(q.label).add(String.format("kafka/%-8s skipped (DATE_FORMAT needs plain TIMESTAMP)", format));
              continue;
            }
            double flink = kafkaBest(brokers, format, Rung.FLINK, q);
            StringBuilder cell = new StringBuilder();
            cell.append(String.format("kafka/%-8s Flink %6.3fs", format, flink));
            for (Rung rung : new Rung[] {Rung.JVM_TRANSPOSE, Rung.RUST_DECODE, Rung.RUST_SOURCE}) {
              double s = kafkaBest(brokers, format, rung, q);
              cell.append(String.format("  | %s %6.3fs %.2fx", rung.label, s, flink / s));
            }
            report.get(q.label).add(cell.toString());
          }
        }
      }
    }

    StringBuilder out = new StringBuilder("\n##### NEXMARK MATRIX (" + ROWS + " events, best of " + RUNS + ") #####\n");
    for (Query q : queries) {
      out.append("\n===== ").append(q.label).append(" =====\n");
      for (String line : report.get(q.label)) {
        out.append("  ").append(line).append('\n');
      }
    }
    System.out.println(out);
  }

  private static Query[] selectQueries() {
    String subset = System.getenv("SF_MATRIX_QUERIES");
    if (subset == null) {
      return ALL_QUERIES;
    }
    Set<String> wanted = Set.copyOf(Arrays.asList(subset.split(",")));
    List<Query> picked = new ArrayList<>();
    for (Query q : ALL_QUERIES) {
      if (wanted.contains(q.label)) {
        picked.add(q);
      }
    }
    return picked.toArray(new Query[0]);
  }

  private static String cell(String source, double flink, double nativeRun) {
    return String.format(
        "%-10s Flink %6.3fs (%,.0f ev/s)  |  Native %6.3fs (%,.0f ev/s)  %.2fx",
        source, flink, ROWS / flink, nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  // ----- generator source -----

  private static double generatorBest(Query q, boolean nativeRun) throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = withDecimal(q, nativeRun, () -> runGeneratorOnce(q, nativeRun));
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runGeneratorOnce(Query q, boolean nativeRun) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    if (q.extraView != null) {
      tEnv.executeSql(q.extraView);
    }
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP(3)");
  }

  // ----- kafka source -----

  private static double kafkaBest(String brokers, String format, Rung rung, Query q) throws Exception {
    Map<String, String> previous = new LinkedHashMap<>();
    boolean nativeRun = !"false".equals(rung.properties.get("streamfusion.native.enabled"));
    Map<String, String> props = new LinkedHashMap<>(rung.properties);
    if (nativeRun && q.approximateDecimal) {
      props.put("streamfusion.expression.decimalArithmetic.approximate", "true");
    }
    props.forEach((k, v) -> previous.put(k, System.getProperty(k)));
    props.forEach(System::setProperty);
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < WARMUP + RUNS; run++) {
        double seconds = runKafkaOnce(brokers, format, nativeRun, q);
        if (run >= WARMUP) {
          best = Math.min(best, seconds);
        }
      }
      return best;
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

  private static double runKafkaOnce(String brokers, String format, boolean nativeRun, Query q)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ", rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3),"
            + " WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND"
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
    // The same person/auction/bid logical streams the published Nexmark queries read, off the
    // watermarked event-time rowtime. expires becomes a timestamp too so q4/q9's BETWEEN typechecks.
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW person AS SELECT person.id AS id, person.name AS name,"
            + " person.emailAddress AS emailAddress, person.creditCard AS creditCard, person.city AS"
            + " city, person.state AS state, rowtime AS `dateTime`, person.extra AS extra FROM src"
            + " WHERE event_type = 0");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW auction AS SELECT auction.id AS id, auction.itemName AS itemName,"
            + " auction.description AS description, auction.initialBid AS initialBid, auction.reserve"
            + " AS reserve, rowtime AS `dateTime`, TO_TIMESTAMP_LTZ(auction.expires, 3) AS expires,"
            + " auction.seller AS seller, auction.category AS category, auction.extra AS extra FROM"
            + " src WHERE event_type = 1");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bid AS SELECT bid.auction AS auction, bid.bidder AS bidder, bid.price"
            + " AS price, bid.channel AS channel, bid.url AS url, rowtime AS `dateTime`, bid.extra AS"
            + " extra FROM src WHERE event_type = 2");
    if (q.extraView != null) {
      tEnv.executeSql(q.extraView);
    }
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP_LTZ(3)");
  }

  // ----- shared -----

  private static double execute(
      TableEnvironment tEnv, PhysicalPlanScan scan, Query q, boolean nativeRun, String tsType)
      throws Exception {
    // %TS% = the event-time rowtime passthrough (plain TIMESTAMP on the generator, TIMESTAMP_LTZ off
    // the Kafka epoch decode). %WTS% = a window-boundary column (window_start/_end, SESSION_START/_END),
    // which Flink always types as a plain TIMESTAMP even over an LTZ rowtime.
    tEnv.executeSql(q.sinkDdl.replace("%TS%", tsType).replace("%WTS%", "TIMESTAMP(3)"));
    long start = System.nanoTime();
    tEnv.executeSql(q.insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun && scan.substitutions() == 0) {
      throw new IllegalStateException(
          q.label + ": native island did not engage; comparison is moot. " + scan.fallbackReasons());
    }
    return seconds;
  }

  @FunctionalInterface
  private interface Run {
    double get() throws Exception;
  }

  private static double withDecimal(Query q, boolean nativeRun, Run run) throws Exception {
    String property = "streamfusion.expression.decimalArithmetic.approximate";
    if (!(nativeRun && q.approximateDecimal)) {
      return run.get();
    }
    String previous = System.getProperty(property);
    System.setProperty(property, "true");
    try {
      return run.get();
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }
}
