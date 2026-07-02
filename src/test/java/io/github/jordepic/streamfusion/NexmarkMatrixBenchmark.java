package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * RowData), a local Parquet file (a columnar file source read straight to Arrow, no ingest transpose),
 * and Kafka json/avro/protobuf, the Kafka formats climbing the source→columnar ladder (JVM transpose,
 * Rust decode with a JVM poll, fully native Rust poll+decode). One table per query, a native cell per
 * source, each a speedup over the Flink baseline for that same source.
 *
 * <p>The Parquet source is the columnar-source case: the same wide event row is written once to a local
 * Parquet directory, then read back through the native {@code filesystem}/{@code parquet} scan — the
 * rows never become {@code RowData} at ingest, so a fully native pipeline pays only the sink transpose.
 * Its rowtime is a plain {@code TIMESTAMP(3)} (unlike the Kafka {@code TIMESTAMP_LTZ}), so the {@code
 * DATE_FORMAT}/{@code HOUR} queries that are generator-only on Kafka run here too.
 *
 * <p>The query set is every query StreamFusion accelerates: q0–q5, q7–q23 (q1's and q14's decimal are
 * exact and native by default; q21's REGEXP_EXTRACT/LOWER and q14's HOUR route through the host
 * implementation via the columnar JVM upcall; q13 is a synchronous lookup join against a bounded
 * {@code test-lookup} dimension). Only q6 is out — Flink itself cannot run it (ticket 39). q10/q14/q15/
 * q16/q17 report on the generator only, since their {@code DATE_FORMAT}/{@code HOUR} need a plain
 * {@code TIMESTAMP} and would partially fall back over the Kafka {@code TIMESTAMP_LTZ} rowtime.
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
 * ({@code false} to skip the generator column), {@code SF_MATRIX_PARQUET} ({@code false} to skip the
 * Parquet column), {@code SF_MATRIX_KAFKA} ({@code false} to skip Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkMatrixBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 500_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;

  // The opt-in native path for DATE_FORMAT/EXTRACT over TIMESTAMP_LTZ (chrono-tz in Rust instead of the
  // byte-parity JVM upcall) — reported as a second "incompatible" row for the datetime queries, exactly
  // as q21 reports its native-regex path. Divergence surface: tzdb-version skew, DST beyond ~2100, deep
  // history, and legacy zone forms (which the encoder rejects → fall back).
  private static final String DATETIME_VARIANT = "native datetime (incompatible)";
  private static final Map<String, String> ALLOW_INCOMPATIBLE =
      Map.of("streamfusion.expression.allowIncompatible", "true");

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
    final String[] setup; // extra SQL run before the insert (q12 proctime view, q13 dim+proctime); else null
    final String sinkDdl; // %TS% substituted with the harness's event-time type
    final String insertSql;
    // A second native measurement run with these extra properties set — used to report a query both on
    // its byte-identical default path and on a faster opt-in path that diverges from Flink at an edge.
    // q21's REGEXP_EXTRACT/LOWER default to a Flink-parity JVM upcall; allowIncompatible switches them to
    // the pure-native Rust regex/case path. Null for queries with no such variant.
    final String nativeVariantLabel;
    final Map<String, String> nativeVariantProps;

    Query(String label, boolean approximateDecimal, String[] setup, String sinkDdl, String insertSql) {
      this(label, approximateDecimal, setup, sinkDdl, insertSql, null, null);
    }

    Query(
        String label,
        boolean approximateDecimal,
        String[] setup,
        String sinkDdl,
        String insertSql,
        String nativeVariantLabel,
        Map<String, String> nativeVariantProps) {
      this.label = label;
      this.approximateDecimal = approximateDecimal;
      this.setup = setup;
      this.sinkDdl = sinkDdl;
      this.insertSql = insertSql;
      this.nativeVariantLabel = nativeVariantLabel;
      this.nativeVariantProps = nativeVariantProps;
    }
  }

  /** Nexmark q14's UDF ({@code count_char(extra, 'c')}); registered in every matrix environment. */
  public static class CountChar extends org.apache.flink.table.functions.ScalarFunction {
    public long eval(String s, String c) {
      if (s == null || c == null || c.isEmpty()) {
        return 0L;
      }
      long count = 0;
      char target = c.charAt(0);
      for (int i = 0; i < s.length(); i++) {
        if (s.charAt(i) == target) {
          count++;
        }
      }
      return count;
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
        false, // exact Decimal128 * + HALF_UP cast is native + byte-parity by default (the reported cell)
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, 0.908 * price AS price, `dateTime`, extra FROM bid",
        // …and a second cell on the faster approximate-decimal path (double math, diverges from Flink's
        // exact rounding at an edge) — same parity-vs-non-parity split as q21's regex/case.
        "approximate decimal (incompatible)",
        Map.of("streamfusion.expression.decimalArithmetic.approximate", "true")),
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
            + " DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm') FROM bid",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
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
        new String[] {"CREATE TEMPORARY VIEW bid_proc AS SELECT *, PROCTIME() AS p_time FROM bid"},
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
            + " rank3_auctions FROM bid GROUP BY DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
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
            + " DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
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
            + " 'yyyy-MM-dd')",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
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
    new Query(
        "q5",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, num BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT AuctionBids.auction, AuctionBids.num FROM (SELECT auction, count(*) AS"
            + " num, window_start AS starttime, window_end AS endtime FROM TABLE(HOP(TABLE bid,"
            + " DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY auction,"
            + " window_start, window_end) AS AuctionBids JOIN (SELECT max(CountBids.num) AS maxn,"
            + " CountBids.starttime, CountBids.endtime FROM (SELECT count(*) AS num, window_start AS"
            + " starttime, window_end AS endtime FROM TABLE(HOP(TABLE bid, DESCRIPTOR(`dateTime`),"
            + " INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY auction, window_start, window_end)"
            + " AS CountBids GROUP BY CountBids.starttime, CountBids.endtime) AS MaxBids ON"
            + " AuctionBids.starttime = MaxBids.starttime AND AuctionBids.endtime = MaxBids.endtime AND"
            + " AuctionBids.num >= MaxBids.maxn"),
    new Query(
        "q13",
        false,
        new String[] {
          "CREATE TEMPORARY VIEW bid_lookup AS SELECT *, PROCTIME() AS p_time FROM bid",
          "CREATE TABLE dim (k BIGINT, val STRING) WITH ('connector' = 'test-lookup')",
        },
        "CREATE TABLE sink (auction BIGINT, price BIGINT, val STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.auction, B.price, D.val FROM bid_lookup AS B JOIN dim"
            + " FOR SYSTEM_TIME AS OF B.p_time AS D ON MOD(B.auction, 5) = D.k"),
    new Query(
        "q14",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), bidTimeType STRING,"
            + " `dateTime` %TS%, extra STRING, c_counts BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, 0.908 * price AS price, CASE WHEN HOUR(`dateTime`) >="
            + " 8 AND HOUR(`dateTime`) <= 18 THEN 'dayTime' WHEN HOUR(`dateTime`) <= 6 OR"
            + " HOUR(`dateTime`) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bidTimeType,"
            + " `dateTime`, extra, count_char(extra, 'c') AS c_counts FROM bid",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
    new Query(
        "q21",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING,"
            + " channel_id STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple'"
            + " THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN"
            + " '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url,"
            + " '(&|^)channel_id=([^&]*)', 2) END AS channel_id FROM bid WHERE REGEXP_EXTRACT(url,"
            + " '(&|^)channel_id=([^&]*)', 2) IS NOT NULL OR lower(channel) IN ('apple', 'google',"
            + " 'facebook', 'baidu')",
        "native regex/case (incompatible)",
        Map.of("streamfusion.expression.allowIncompatible", "true")),
    new Query(
        "q23",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, itemName STRING,"
            + " auction_dateTime %TS%, seller BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.auction, B.bidder, B.price, A.itemName, A.`dateTime`, A.seller"
            + " FROM bid B JOIN person P ON P.id = B.bidder JOIN auction A ON A.seller = B.bidder"),
  };

  // The wide nested event row written to / read from Parquet. Same shape as the generator's event row,
  // with a plain TIMESTAMP(3) rowtime (so DATE_FORMAT/HOUR stay native, unlike the Kafka LTZ rowtime).
  private static final String PARQUET_SCHEMA =
      "event_type INT,"
          + " person ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING,"
          + " state STRING, `dateTime` TIMESTAMP(3), extra STRING>,"
          + " auction ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
          + " reserve BIGINT, `dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT,"
          + " category BIGINT, extra STRING>,"
          + " bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
          + " `dateTime` TIMESTAMP(3), extra STRING>,"
          + " `dateTime` TIMESTAMP(3)";

  @Test
  void matrix() throws Exception {
    Query[] queries = selectQueries();
    boolean runGenerator = !"false".equals(System.getenv("SF_MATRIX_GENERATOR"));
    boolean runParquet = !"false".equals(System.getenv("SF_MATRIX_PARQUET"));
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
        double flink = generatorBest(q, false, null);
        double nativeRun = generatorBest(q, true, null);
        report.get(q.label).add(cell("generator", flink, nativeRun));
        // A query with a faster opt-in path that diverges from Flink at an edge (q21's native
        // regex/case) reports it too, against the same Flink baseline — parity and non-parity side by
        // side, so the cost of staying byte-identical is visible.
        if (q.nativeVariantProps != null) {
          double variant = generatorBest(q, true, q.nativeVariantProps);
          report.get(q.label).add(variantCell("generator", q.nativeVariantLabel, flink, variant));
        }
      }
    }

    if (runParquet) {
      Path dir = writeParquetSource();
      for (Query q : queries) {
        double flink = parquetBest(dir, q, false, null);
        double nativeRun = parquetBest(dir, q, true, null);
        report.get(q.label).add(cell("parquet", flink, nativeRun));
        if (q.nativeVariantProps != null) {
          double variant = parquetBest(dir, q, true, q.nativeVariantProps);
          report.get(q.label).add(variantCell("parquet", q.nativeVariantLabel, flink, variant));
        }
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
            double flink = kafkaBest(brokers, format, Rung.FLINK, q, null);
            report.get(q.label).add(kafkaCell(brokers, format, q, flink, null, null));
            // The opt-in path (q21 native regex/case; q10/q14/q15/q16/q17 native chrono-tz datetime) is
            // measured on Kafka too, so the incompatible row has per-format numbers like the default.
            if (q.nativeVariantProps != null) {
              report
                  .get(q.label)
                  .add(kafkaCell(brokers, format, q, flink, q.nativeVariantLabel, q.nativeVariantProps));
            }
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

  /**
   * Runs one query (default q19, override with {@code -Dprofile.query}) natively on the generator in a
   * loop for {@code -Dprofile.seconds} (default 60), so an attached sampler sees steady-state of the
   * changelog operator that query is bound by — no Kafka, no decode, just transpose → native island.
   * Gated by {@code SF_PROFILE=true} on top of {@code SF_BENCHMARK}; attach async-profiler to the fork.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE", matches = "true")
  void generatorNativeProfileLoop() throws Exception {
    String label = System.getProperty("profile.query", "q19");
    Query q =
        Arrays.stream(ALL_QUERIES)
            .filter(x -> x.label.equals(label))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown profile.query: " + label));
    // profile.native=false profiles the stock-Flink path instead, so the two can be diffed to isolate
    // what the native island actually spends beyond what Flink already pays (source/decode are shared).
    boolean nativeRun = !"false".equals(System.getProperty("profile.native", "true"));
    long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
    long iterations = 0;
    Map<String, String> variant =
        "true".equals(System.getProperty("profile.variant")) ? q.nativeVariantProps : null;
    while (System.currentTimeMillis() < deadline) {
      withProps(q, nativeRun, variant, () -> runGeneratorOnce(q, nativeRun));
      iterations++;
    }
    System.out.println("[profile] " + (nativeRun ? "native " : "flink ") + label + " iterations: " + iterations);
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

  private static void runSetup(TableEnvironment tEnv, Query q) {
    if (q.setup != null) {
      for (String statement : q.setup) {
        tEnv.executeSql(statement);
      }
    }
  }

  private static String cell(String source, double flink, double nativeRun) {
    return String.format(
        "%-10s Flink %6.3fs (%,.0f ev/s)  |  Native %6.3fs (%,.0f ev/s)  %.2fx",
        source, flink, ROWS / flink, nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static String variantCell(String source, String label, double flink, double variant) {
    return String.format(
        "%-10s [%s]  Native %6.3fs (%,.0f ev/s)  %.2fx",
        source, label, variant, ROWS / variant, flink / variant);
  }

  // ----- generator source -----

  private static double generatorBest(Query q, boolean nativeRun, Map<String, String> extra)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = withProps(q, nativeRun, extra, () -> runGeneratorOnce(q, nativeRun));
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runGeneratorOnce(Query q, boolean nativeRun) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP(3)");
  }

  // ----- parquet file source -----

  /** Writes the wide event row to a fresh local Parquet directory once; every query reads it back. */
  private static Path writeParquetSource() throws Exception {
    Path dir = Files.createTempDirectory("bench-nexmark-parquet");
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    tEnv.executeSql(
        "CREATE TABLE parquet_write ("
            + PARQUET_SCHEMA
            + ") WITH ('connector' = 'filesystem', 'path' = '"
            + dir.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO parquet_write SELECT event_type, person, auction, bid, `dateTime` FROM events")
        .await();
    return dir;
  }

  private static double parquetBest(Path dir, Query q, boolean nativeRun, Map<String, String> extra)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = withProps(q, nativeRun, extra, () -> runParquetOnce(dir, nativeRun, q));
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runParquetOnce(Path dir, boolean nativeRun, Query q) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + PARQUET_SCHEMA
            + ", WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND"
            + ") WITH ('connector' = 'filesystem', 'path' = '"
            + dir.toUri()
            + "', 'format' = 'parquet')");
    // The same person/auction/bid logical streams as the generator, off the watermarked event-time
    // `dateTime` (a plain TIMESTAMP(3) here, so DATE_FORMAT/HOUR stay native).
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW person AS SELECT person.id AS id, person.name AS name,"
            + " person.emailAddress AS emailAddress, person.creditCard AS creditCard, person.city AS"
            + " city, person.state AS state, `dateTime`, person.extra AS extra FROM src WHERE"
            + " event_type = 0");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW auction AS SELECT auction.id AS id, auction.itemName AS itemName,"
            + " auction.description AS description, auction.initialBid AS initialBid, auction.reserve"
            + " AS reserve, `dateTime`, auction.expires AS expires, auction.seller AS seller,"
            + " auction.category AS category, auction.extra AS extra FROM src WHERE event_type = 1");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bid AS SELECT bid.auction AS auction, bid.bidder AS bidder, bid.price"
            + " AS price, bid.channel AS channel, bid.url AS url, `dateTime`, bid.extra AS extra FROM"
            + " src WHERE event_type = 2");
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP(3)");
  }

  // ----- kafka source -----

  /**
   * One Kafka cell for a query: the Flink baseline plus the three source rungs, under the given extra
   * native props (null = the byte-parity default; the variant props = the allowIncompatible path). The
   * label prefix distinguishes the two rows.
   */
  private static String kafkaCell(
      String brokers,
      String format,
      Query q,
      double flink,
      String variantLabel,
      Map<String, String> extraProps)
      throws Exception {
    StringBuilder cell = new StringBuilder();
    cell.append(
        variantLabel == null
            ? String.format("kafka/%-8s Flink %6.3fs", format, flink)
            : String.format("kafka/%-8s [%s]", format, variantLabel));
    for (Rung rung : new Rung[] {Rung.JVM_TRANSPOSE, Rung.RUST_DECODE, Rung.RUST_SOURCE}) {
      double s = kafkaBest(brokers, format, rung, q, extraProps);
      cell.append(String.format("  | %s %6.3fs %.2fx", rung.label, s, flink / s));
    }
    return cell.toString();
  }

  private static double kafkaBest(
      String brokers, String format, Rung rung, Query q, Map<String, String> extraProps)
      throws Exception {
    Map<String, String> previous = new LinkedHashMap<>();
    boolean nativeRun = !"false".equals(rung.properties.get("streamfusion.native.enabled"));
    Map<String, String> props = new LinkedHashMap<>(rung.properties);
    if (nativeRun && q.approximateDecimal) {
      props.put("streamfusion.expression.decimalArithmetic.approximate", "true");
    }
    if (nativeRun && extraProps != null) {
      props.putAll(extraProps);
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
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
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

  private static double withProps(Query q, boolean nativeRun, Map<String, String> extra, Run run)
      throws Exception {
    Map<String, String> props = new LinkedHashMap<>();
    if (nativeRun && q.approximateDecimal) {
      props.put("streamfusion.expression.decimalArithmetic.approximate", "true");
    }
    if (nativeRun && extra != null) {
      props.putAll(extra);
    }
    if (props.isEmpty()) {
      return run.get();
    }
    Map<String, String> previous = new LinkedHashMap<>();
    props.forEach((k, v) -> previous.put(k, System.getProperty(k)));
    props.forEach(System::setProperty);
    try {
      return run.get();
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
}
