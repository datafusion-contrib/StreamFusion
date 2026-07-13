package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import io.github.jordepic.streamfusion.fluss.NativeFluss;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.fluss.server.testutils.FlussClusterExtension;
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
 * <p>The optional Fluss rung ({@code SF_MATRIX_FLUSS=true}) preloads the same wide event row into a
 * local Fluss test cluster, then reads it back through both Fluss's stock Flink connector and the
 * native fluss-rs log-table source. Both engines run the identical SQL (no limit) in the same
 * default streaming environment the other rungs use; because the Fluss log table is unbounded, each
 * run counts changelog rows in the sink and cancels the job once the Nth row arrives, reporting
 * time-to-Nth-row. N is the query's full output cardinality over the preloaded events, measured once
 * per query with stock Flink on the bounded generator (the same rows the preload wrote) through its
 * watermarked event-time views — the Fluss table declares the identical watermark, and a preloaded
 * far-future sentinel event closes the final windows the generator's end-of-input flush closes — so
 * both engines are cancelled at the same row. Queries with no such deterministic N report a skip
 * cell instead — see {@link #flussSkipReason}.
 *
 * <p>The query set is every query StreamFusion accelerates: q0–q5, q7–q23 (q1's and q14's decimal are
 * exact and native by default; q21's REGEXP_EXTRACT/LOWER and q14's HOUR route through the host
 * implementation via the columnar JVM upcall; q13 is a synchronous lookup join against a bounded
 * {@code test-lookup} dimension). Only q6 is out — Flink itself cannot run it (wontdos/39). q10/q14/q15/
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
 * Parquet column), {@code SF_MATRIX_KAFKA} ({@code false} to skip Kafka), {@code SF_MATRIX_FLUSS}
 * ({@code true} to include the first stock Flink-on-Fluss baseline).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkMatrixBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 500_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;

  // Extra TableConfig entries applied to EVERY measured environment (both engines) — the tuned
  // matrix sets the mini-batch keys here so Flink and the native island run the same tuning,
  // per the steelman rule. Empty for the default matrix.
  private static Map<String, String> tableConfigExtras = Map.of();

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
  private static final String PERSON_TYPE =
      "ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING,"
          + " state STRING, `dateTime` TIMESTAMP(3), extra STRING>";
  private static final String AUCTION_TYPE =
      "ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
          + " reserve BIGINT, `dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT,"
          + " category BIGINT, extra STRING>";
  private static final String BID_TYPE =
      "ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
          + " `dateTime` TIMESTAMP(3), extra STRING>";
  private static final String PARQUET_SCHEMA =
      "event_type INT,"
          + " person " + PERSON_TYPE + ","
          + " auction " + AUCTION_TYPE + ","
          + " bid " + BID_TYPE + ","
          + " `dateTime` TIMESTAMP(3)";
  private static final String FLUSS_CATALOG = "fluss_catalog";
  private static final String FLUSS_TABLE = FLUSS_CATALOG + ".fluss.nexmark_events";
  // A second copy of the preload with a poison auction+bid pair appended after every real event:
  // the pair's output row is the finish line for the queries whose changelog row count is not
  // deterministic (q4/q9) or zero (q21) — in a parallelism-1 pipeline the marker's output is
  // necessarily emitted after all real output, so time-to-marker measures the full drain without
  // needing a row count. The ids are outside every real range (categories are 0..99; auction ids
  // grow ~3 per 50-event block), and the bid's channel is 'apple' — the only row in the stream
  // q21's filter selects, so its marker doubles as its first output.
  private static final String FLUSS_TRACED_TABLE = FLUSS_CATALOG + ".fluss.nexmark_events_traced";
  private static final long POISON_AUCTION_ID = 999_999_999L;
  private static final long POISON_CATEGORY = 999L;
  // Sink-row marker (column 0) per traced query: q4 emits its category, q9/q21 the auction id.
  private static final Map<String, Long> FLUSS_MARKERS =
      Map.of("q4", POISON_CATEGORY, "q9", POISON_AUCTION_ID, "q21", POISON_AUCTION_ID);
  // How long a Fluss run may take to reach its Nth sink row before the benchmark fails loudly (the
  // unbounded source never finishes on its own, so a wrong target must not hang the matrix
  // forever). Healthy 500K-event cells finish in single-digit seconds, so two minutes is a stall,
  // not a slow run.
  private static final long FLUSS_NTH_ROW_TIMEOUT_SECONDS = 120L;

  @Test
  void matrix() throws Exception {
    Query[] queries = selectQueries();
    boolean runGenerator = !"false".equals(System.getenv("SF_MATRIX_GENERATOR"));
    boolean runParquet = !"false".equals(System.getenv("SF_MATRIX_PARQUET"));
    boolean runKafka = !"false".equals(System.getenv("SF_MATRIX_KAFKA"));
    boolean runFluss = "true".equals(System.getenv("SF_MATRIX_FLUSS"));
    String formatsEnv = System.getenv("SF_LADDER_FORMATS");
    String[] formats =
        formatsEnv != null ? formatsEnv.split(",") : new String[] {"json", "avro", "protobuf"};
    if (runFluss && !NativeFluss.featureBuilt()) {
      throw new IllegalArgumentException(
          "SF_MATRIX_FLUSS=true now reports native Fluss vs stock Flink-on-Fluss, so the native "
              + "library must be built with the fluss cargo feature.");
    }

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

    if (runFluss) {
      FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
      cluster.start();
      try {
        String bootstrapServers = cluster.getBootstrapServers();
        writeFlussSource(bootstrapServers);
        if (Arrays.stream(queries).anyMatch(q -> FLUSS_MARKERS.containsKey(q.label))) {
          writeFlussTracedSource(bootstrapServers);
        }
        for (Query q : queries) {
          String skipReason = flussSkipReason(q);
          if (skipReason != null) {
            report.get(q.label).add(skipCell("fluss", skipReason));
            continue;
          }
          // Marker queries cancel on the poison pair's output row, not a row count — no
          // calibration run needed (and none possible: their counts are what's non-deterministic).
          long targetRows = FLUSS_MARKERS.containsKey(q.label) ? -1 : flussTargetRows(q);
          if (targetRows == 0) {
            report
                .get(q.label)
                .add(skipCell("fluss", "query emits no rows over the preloaded events"));
            continue;
          }
          double flink = flussBest(bootstrapServers, q, false, null, targetRows);
          double nativeRun = flussBest(bootstrapServers, q, true, null, targetRows);
          report.get(q.label).add(cell("fluss", flink, nativeRun));
          if (q.nativeVariantProps != null) {
            double variant = flussBest(bootstrapServers, q, true, q.nativeVariantProps, targetRows);
            report.get(q.label).add(variantCell("fluss", q.nativeVariantLabel, flink, variant));
          }
        }
      } finally {
        cluster.close();
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
   * The "tuned Flink" column: the same queries with {@code table.exec.mini-batch.*} enabled on BOTH
   * engines — the standard production tuning for the stateful changelog queries, and the config the
   * only public per-query Alibaba comparison used. Generator source only (the tuned question is
   * engine-vs-engine, not the source perimeter), changelog-family queries by default.
   * {@code table.optimizer.distinct-agg.split.enabled} stays at its default (off): it is a skew
   * mitigation for parallel deployments — these runs are parallelism 1 — and its incremental plan
   * chain has no native path yet (ticket 41). A query whose mini-batch plan shape does not route
   * reports the fallback instead of failing the run, so the column doubles as the mini-batch
   * coverage check. Gated by {@code SF_MATRIX_TUNED=true} on top of {@code SF_BENCHMARK}.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_TUNED", matches = "true")
  void tunedMiniBatchMatrix() throws Exception {
    Map<String, String> miniBatch =
        Map.of(
            "table.exec.mini-batch.enabled", "true",
            "table.exec.mini-batch.allow-latency", "2 s",
            "table.exec.mini-batch.size", "50000");
    Query[] queries = selectTunedQueries();
    StringBuilder out =
        new StringBuilder(
            "\n##### NEXMARK TUNED (mini-batch on both engines; "
                + ROWS
                + " events, best of "
                + RUNS
                + ") #####\n");
    for (Query q : queries) {
      tableConfigExtras = miniBatch;
      try {
        double flink = generatorBest(q, false, null);
        String result;
        try {
          double nativeRun = generatorBest(q, true, null);
          result = cell("tuned", flink, nativeRun);
        } catch (IllegalStateException fallback) {
          result = String.format("tuned      Flink %6.3fs  |  %s", flink, fallback.getMessage());
        }
        out.append(String.format("%-4s  %s%n", q.label, result));
      } finally {
        tableConfigExtras = Map.of();
      }
    }
    System.out.println(out);
  }

  /**
   * Direct enabled-versus-disabled comparison for both engines on the same 5M-style generator
   * workload. Unlike running {@link #matrix()} and {@link #tunedMiniBatchMatrix()} back to back,
   * this keeps the two modes adjacent for each query and alternates which mode runs first. That
   * balances long-lived JVM/GC order effects while retaining the normal warmup and best-of-two rule
   * inside every cell. Gated by {@code SF_MATRIX_COMPARE_MODES=true}.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_COMPARE_MODES", matches = "true")
  void miniBatchModeComparison() throws Exception {
    Map<String, String> miniBatch =
        Map.of(
            "table.exec.mini-batch.enabled", "true",
            "table.exec.mini-batch.allow-latency", "2 s",
            "table.exec.mini-batch.size", "50000");
    Query[] queries = selectQueries();
    StringBuilder out =
        new StringBuilder(
            "\n##### NEXMARK MINI-BATCH MODE COMPARISON ("
                + ROWS
                + " events, best of "
                + RUNS
                + ") #####\n");
    out.append(
        "query  Flink off  Native off  SF/Flink off  Flink on  Native on  SF/Flink on  Flink on/off  SF on/off\n");

    for (int i = 0; i < queries.length; i++) {
      Query q = queries[i];
      double flinkOff;
      double nativeOff;
      double flinkOn;
      double nativeOn;
      if ((i & 1) == 0) {
        flinkOff = generatorBestWithConfig(q, false, Map.of());
        nativeOff = generatorBestWithConfig(q, true, Map.of());
        flinkOn = generatorBestWithConfig(q, false, miniBatch);
        nativeOn = generatorBestWithConfig(q, true, miniBatch);
      } else {
        flinkOn = generatorBestWithConfig(q, false, miniBatch);
        nativeOn = generatorBestWithConfig(q, true, miniBatch);
        flinkOff = generatorBestWithConfig(q, false, Map.of());
        nativeOff = generatorBestWithConfig(q, true, Map.of());
      }
      out.append(
          String.format(
              "%4s  %9.3f  %10.3f  %12.2fx  %8.3f  %9.3f  %11.2fx  %12.2fx  %9.2fx%n",
              q.label,
              ROWS / flinkOff / 1_000_000.0,
              ROWS / nativeOff / 1_000_000.0,
              flinkOff / nativeOff,
              ROWS / flinkOn / 1_000_000.0,
              ROWS / nativeOn / 1_000_000.0,
              flinkOn / nativeOn,
              flinkOff / flinkOn,
              nativeOff / nativeOn));
    }
    System.out.println(out);
  }

  private static double generatorBestWithConfig(
      Query q, boolean nativeRun, Map<String, String> config) throws Exception {
    tableConfigExtras = config;
    try {
      return generatorBest(q, nativeRun, null);
    } finally {
      tableConfigExtras = Map.of();
    }
  }

  /** The changelog-family queries (mini-batch has no effect on the windowed ones), unless overridden. */
  private static Query[] selectTunedQueries() {
    if (System.getenv("SF_MATRIX_QUERIES") != null) {
      return selectQueries();
    }
    Set<String> family = Set.of("q3", "q4", "q9", "q15", "q16", "q17", "q18", "q19", "q20", "q23");
    return Arrays.stream(ALL_QUERIES).filter(q -> family.contains(q.label)).toArray(Query[]::new);
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
    // -Dprofile.minibatch=true profiles the tuned (mini-batch) plan shape instead of the default.
    if ("true".equals(System.getProperty("profile.minibatch"))) {
      tableConfigExtras =
          Map.of(
              "table.exec.mini-batch.enabled", "true",
              "table.exec.mini-batch.allow-latency", "2 s",
              "table.exec.mini-batch.size", "50000");
    }
    try {
      while (System.currentTimeMillis() < deadline) {
        withProps(q, nativeRun, variant, () -> runGeneratorOnce(q, nativeRun));
        iterations++;
      }
    } finally {
      tableConfigExtras = Map.of();
    }
    System.out.println("[profile] " + (nativeRun ? "native " : "flink ") + label + " iterations: " + iterations);
  }

  /**
   * Runs one query (default q0, override with {@code -Dprofile.query}) against a preloaded local
   * Fluss cluster in a loop for {@code -Dprofile.seconds} (default 60), so an attached sampler
   * sees the native fluss-rs source's steady state — poll, decode, FFI export, and whatever the
   * query chains on top. {@code -Dprofile.native=false} profiles the stock Flink-on-Fluss path
   * instead, for the differential read. Gated by {@code SF_PROFILE_FLUSS=true} on top of {@code
   * SF_BENCHMARK}; attach async-profiler to the fork (see docs/benchmarks.md).
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_FLUSS", matches = "true")
  void flussNativeProfileLoop() throws Exception {
    String label = System.getProperty("profile.query", "q0");
    Query q =
        Arrays.stream(ALL_QUERIES)
            .filter(x -> x.label.equals(label))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown profile.query: " + label));
    boolean nativeRun = !"false".equals(System.getProperty("profile.native", "true"));
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    try {
      cluster.start();
      String bootstrapServers = cluster.getBootstrapServers();
      writeFlussSource(bootstrapServers);
      if (FLUSS_MARKERS.containsKey(label)) {
        writeFlussTracedSource(bootstrapServers);
      }
      long targetRows = FLUSS_MARKERS.containsKey(label) ? -1 : flussTargetRows(q);
      long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
      long iterations = 0;
      while (System.currentTimeMillis() < deadline) {
        withProps(q, nativeRun, null, () -> runFlussOnce(bootstrapServers, nativeRun, q, targetRows));
        iterations++;
      }
      System.out.println(
          "[profile] fluss " + (nativeRun ? "native " : "flink ") + label + " iterations: " + iterations);
    } finally {
      cluster.close();
    }
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

  private static String skipCell(String source, String reason) {
    return String.format("%-10s skipped (%s)", source, reason);
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
    tableConfigExtras.forEach((k, v) -> tEnv.getConfig().getConfiguration().setString(k, v));
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP(3)");
  }

  // ----- Fluss source -----

  /**
   * Writes the wide event row to a local Fluss log table once; every Fluss query reads it back. The
   * table declares the same 4s bounded-out-of-orderness WATERMARK the generator uses, so the
   * windowed queries have a time attribute on both engines (the watermark runs natively as the
   * columnar assigner above the native source). After the events, one sentinel row with a
   * far-future rowtime and an
   * event_type outside 0..2 is appended: it is filtered from every person/auction/bid view but
   * advances the table's watermark past every real window end, so the unbounded Fluss runs emit
   * exactly the rows the bounded generator calibration counted (whose end-of-input flush plays the
   * same role).
   */
  private static void writeFlussSource(String bootstrapServers) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    createFlussCatalog(tEnv, bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + FLUSS_TABLE);
    tEnv.executeSql(
        "CREATE TABLE "
            + FLUSS_TABLE
            + " ("
            + PARQUET_SCHEMA
            + ", WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND"
            + ") WITH ('bucket.num' = '1')");
    tEnv.executeSql(
            "INSERT INTO "
                + FLUSS_TABLE
                + " SELECT event_type, person, auction, bid, `dateTime` FROM events")
        .await();
    // The Fluss sink rejects partial-column inserts on a non-PK table, so the sentinel spells out
    // every column with NULL-cast structs.
    tEnv.executeSql(
            "INSERT INTO "
                + FLUSS_TABLE
                + " SELECT CAST(99 AS INT), CAST(NULL AS "
                + PERSON_TYPE
                + "), CAST(NULL AS "
                + AUCTION_TYPE
                + "), CAST(NULL AS "
                + BID_TYPE
                + "), TIMESTAMP '2100-01-01 00:00:00'")
        .await();
  }

  /**
   * The traced copy for the marker queries ({@link #FLUSS_MARKERS}): the same events plus a poison
   * auction+bid pair appended last (2099, after every 2024 rowtime), whose output row is the
   * cancel condition — no watermark sentinel needed, since nothing here waits on a window.
   */
  private static void writeFlussTracedSource(String bootstrapServers) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    createFlussCatalog(tEnv, bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + FLUSS_TRACED_TABLE);
    tEnv.executeSql(
        "CREATE TABLE "
            + FLUSS_TRACED_TABLE
            + " ("
            + PARQUET_SCHEMA
            + ", WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND"
            + ") WITH ('bucket.num' = '1')");
    tEnv.executeSql(
            "INSERT INTO "
                + FLUSS_TRACED_TABLE
                + " SELECT event_type, person, auction, bid, `dateTime` FROM events")
        .await();
    tEnv.executeSql(
            "INSERT INTO "
                + FLUSS_TRACED_TABLE
                + " SELECT CAST(1 AS INT), CAST(NULL AS "
                + PERSON_TYPE
                + "), CAST(ROW("
                + POISON_AUCTION_ID
                + ", 'poison-item', 'poison', 1, 1, TIMESTAMP '2099-01-01 00:00:00',"
                + " TIMESTAMP '2099-01-02 00:00:00', 1, "
                + POISON_CATEGORY
                + ", 'x') AS "
                + AUCTION_TYPE
                + "), CAST(NULL AS "
                + BID_TYPE
                + "), TIMESTAMP '2099-01-01 00:00:00'")
        .await();
    tEnv.executeSql(
            "INSERT INTO "
                + FLUSS_TRACED_TABLE
                + " SELECT CAST(2 AS INT), CAST(NULL AS "
                + PERSON_TYPE
                + "), CAST(NULL AS "
                + AUCTION_TYPE
                + "), CAST(ROW("
                + POISON_AUCTION_ID
                + ", 1, 100, 'apple', 'https://nexmark.test/poison',"
                + " TIMESTAMP '2099-01-01 00:00:01', 'x') AS "
                + BID_TYPE
                + "), TIMESTAMP '2099-01-01 00:00:01'")
        .await();
  }

  private static double flussBest(
      String bootstrapServers, Query q, boolean nativeRun, Map<String, String> extra, long targetRows)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds =
          withProps(
              q, nativeRun, extra, () -> runFlussOnce(bootstrapServers, nativeRun, q, targetRows));
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  /**
   * One Fluss run: both engines get the identical SQL over the unbounded log table in the same
   * default streaming environment the other rungs use. The log table never reaches end-of-input, so
   * instead of awaiting the insert the query streams into a {@link CountingSink} and the job is
   * cancelled once the {@code targetRows}th changelog row arrives — the elapsed time-to-Nth-row is
   * the measurement (submission and startup included, like the other rungs' await).
   */
  private static double runFlussOnce(
      String bootstrapServers, boolean nativeRun, Query q, long targetRows) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    createFlussCatalog(tEnv, bootstrapServers);
    Long marker = FLUSS_MARKERS.get(q.label);
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW src AS SELECT * FROM "
            + (marker != null ? FLUSS_TRACED_TABLE : FLUSS_TABLE));
    createEventViews(tEnv);
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    // The same INSERT INTO sink shape as every other rung, with the counting blackhole standing in
    // for blackhole: the sink swallows raw RowData (no external-Row conversion — the previous
    // toChangelogStream sink added one heavy enough to compress every ratio) while still giving the
    // driver its finish line, the Nth changelog row or the poison marker's output row.
    tEnv.executeSql(
        q.sinkDdl
            .replace("'connector' = 'blackhole'", "'connector' = 'counting-blackhole'")
            .replace("%TS%", "TIMESTAMP(3)")
            .replace("%WTS%", "TIMESTAMP(3)"));
    CountingBlackholeTableFactory.arm(targetRows, marker);
    long start = System.nanoTime();
    JobClient job =
        tEnv.executeSql(q.insertSql)
            .getJobClient()
            .orElseThrow(() -> new IllegalStateException("insert produced no job"));
    try {
      if (nativeRun && scan.substitutions() == 0) {
        throw new IllegalStateException(
            q.label + ": native island did not engage; comparison is moot. " + scan.fallbackReasons());
      }
      if (!CountingBlackholeTableFactory.targetReached.await(
          FLUSS_NTH_ROW_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new TimeoutException(
            q.label
                + ": Fluss run saw "
                + CountingBlackholeTableFactory.rowsSeen.get()
                + " sink rows and no finish line ("
                + (FLUSS_MARKERS.containsKey(q.label)
                    ? "marker row " + FLUSS_MARKERS.get(q.label)
                    : targetRows + " rows")
                + ") within "
                + FLUSS_NTH_ROW_TIMEOUT_SECONDS
                + "s");
      }
      return (System.nanoTime() - start) / 1e9;
    } finally {
      try {
        job.cancel().get();
      } catch (Exception ignored) {
        // The job may already be terminating (e.g. it failed before the Nth row); the exception
        // propagating out of the try block is the interesting one, so cancellation noise stays here.
      }
      CountingBlackholeTableFactory.disarm();
    }
  }

  /**
   * The number of changelog rows {@code q} emits over the preloaded events — measured once per query
   * with stock Flink on the bounded generator (the same rows the Fluss preload wrote) through its
   * own watermarked event-time views (the Fluss table declares the identical 4s watermark), so both
   * engines are cancelled at the same Nth sink row. The generator's end-of-input flush closes the
   * final windows here; the preloaded sentinel row closes the same windows on the unbounded Fluss
   * runs, so the counts line up.
   */
  private static long flussTargetRows(Query q) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    long rows = 0;
    try (CloseableIterator<Row> it =
        tEnv.executeSql(q.insertSql.substring("INSERT INTO sink ".length())).collect()) {
      while (it.hasNext()) {
        it.next();
        rows++;
      }
    }
    return rows;
  }

  /** The person/auction/bid logical streams over a wide-event {@code src} with a plain TIMESTAMP. */
  private static void createEventViews(TableEnvironment tEnv) {
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
  }

  private static void createFlussCatalog(TableEnvironment tEnv, String bootstrapServers) {
    tEnv.executeSql(
        "CREATE CATALOG "
            + FLUSS_CATALOG
            + " WITH ('type' = 'fluss', 'bootstrap.servers' = '"
            + bootstrapServers
            + "')");
  }

  /**
   * Why a query cannot run on the Fluss rung, or null when it can. Only q12 is out: a proctime
   * window's output count is wall-clock-dependent, and any marker's own window would close ~10s
   * (the window size) after the drain, so a finish line would time the window, not the engines.
   * The queries whose changelog row count is not deterministic (q4/q9 — the join-input
   * interleaving decides how many -U/+U pairs the update-collapsing aggregate/rank emits) or zero
   * (q21) run against the traced table and cancel on the poison marker's output row instead of a
   * row count ({@link #FLUSS_MARKERS}).
   */
  private static String flussSkipReason(Query q) {
    if ("q12".equals(q.label)) {
      return "proctime windows have no deterministic output count to cancel at";
    }
    return null;
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
