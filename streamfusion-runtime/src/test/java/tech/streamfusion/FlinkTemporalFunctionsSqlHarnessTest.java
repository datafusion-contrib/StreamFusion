package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkTemporalFunctionsSqlHarnessTest {
  @Test
  void parsesTimestampsAndFormatsDynamicPatterns() throws Exception {
    parity(
        "TO_TIMESTAMP(s), TO_TIMESTAMP(s, p), DATE_FORMAT(ts, p), DATE_FORMAT(ltz, p),"
            + " DATE_FORMAT(ts, 'EEEE MMMM dd yyyy'), DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss')");
  }

  @Test
  void epochConversionsComposeWithTimestampExpressions() throws Exception {
    parity(
        "TO_TIMESTAMP_LTZ(epoch, 3), "
            + "TO_TIMESTAMP_LTZ(n, 0), TO_TIMESTAMP_LTZ(CAST(epoch AS DECIMAL(18, 3)), 3), "
            + "EXTRACT(SECOND FROM TO_TIMESTAMP_LTZ(epoch, 3)), "
            + "UNIX_TIMESTAMP(s), UNIX_TIMESTAMP(s, p), FROM_UNIXTIME(n), FROM_UNIXTIME(n, p)");
  }

  @Test
  void convertsTimeZonesAndParsesFormattedDates() throws Exception {
    parity("CONVERT_TZ(s, 'UTC', z), TO_DATE(s, p)");
  }

  @ParameterizedTest
  @ValueSource(strings = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND"})
  void temporalRoundingPreservesTypesAndNestedExpressions(String unit) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.SUB_HOUR_TIMESTAMP_ROUNDING
            || java.util.Set.of("YEAR", "MONTH", "DAY", "HOUR").contains(unit),
        "Flink 1.18 emits an invalid TimestampData floor/ceil call below HOUR precision");
    parity(
        "FLOOR(ts TO "
            + unit
            + "), CEIL(ts TO "
            + unit
            + "), "
            + "FLOOR(ltz TO "
            + unit
            + "), CEILING(ltz TO "
            + unit
            + "), "
            + "EXTRACT(YEAR FROM FLOOR(ts TO "
            + unit
            + "))");
  }

  @Test
  void extractsDateAndTimeFields() throws Exception {
    parity(
        "YEAR(d), MONTH(d), DAYOFMONTH(d), QUARTER(d), WEEK(d), DAYOFYEAR(d), DAYOFWEEK(d), "
            + "HOUR(tm), MINUTE(tm), SECOND(tm), EXTRACT(MILLISECOND FROM ts), "
            + "YEAR(ltz), QUARTER(ltz), MONTH(ltz), WEEK(ltz), DAYOFYEAR(ltz), DAYOFWEEK(ltz)");
  }

  @Test
  void roundsDatesAndTimesAndExtractsIntervals() throws Exception {
    parity(
        "FLOOR(tm TO MINUTE), CAST(CEIL(tm TO MINUTE) AS STRING), FLOOR(d TO MONTH), CEIL(d TO"
            + " YEAR), EXTRACT(DAY FROM n * INTERVAL '1' DAY), EXTRACT(MONTH FROM n * INTERVAL '1'"
            + " MONTH), TIMESTAMPADD(DAY, n, d), TIMESTAMPDIFF(DAY, d, DATE '2024-03-31')");
  }

  @Test
  void addsIntervalsAndCalculatesDifferences() throws Exception {
    parity(
        "TIMESTAMPADD(MONTH, n, ts), TIMESTAMPADD(SECOND, n, ts), "
            + "TIMESTAMPDIFF(MONTH, ts, TIMESTAMP '2024-03-31 00:00:00'), "
            + "TIMESTAMPDIFF(SECOND, ts, TIMESTAMP '2024-03-31 00:00:00'), "
            + "ts + INTERVAL '1' MONTH, ts - INTERVAL '1' DAY, "
            + "d + INTERVAL '1' MONTH, tm + INTERVAL '1' HOUR");
  }

  @Test
  void castsTemporalValuesAndKeepsSubMillisecondPrecision() throws Exception {
    parity(
        "CAST(ts AS TIMESTAMP(3)), CAST(ts AS DATE), "
            + "CAST(ts AS STRING), CAST(d AS STRING), CAST(tm AS STRING), CAST(ltz AS STRING), "
            + "CAST(ts AS TIMESTAMP_LTZ(9)), CAST(ltz AS TIMESTAMP(9)), CAST(s AS TIMESTAMP(3))");
  }

  @Test
  void temporalCastsRespectLegacyBehaviorFromTheEnvironmentConfiguration() throws Exception {
    NativeParity.assertParity(
        () -> {
          var tables = environment();
          var root = org.apache.flink.table.api.TableConfig.getDefault();
          root.setRootConfiguration(tables.getConfig().getRootConfiguration());
          root.set("table.exec.legacy-cast-behaviour", "ENABLED");
          tables.getConfig().setRootConfiguration(root);
          return tables;
        },
        "SELECT CAST(s AS TIMESTAMP(3)), CAST(ts AS STRING), CAST(tm AS STRING) FROM"
            + " temporal_inputs");
  }

  @Test
  void temporalLiteralsAndOverlapsRemainInsideNativeCalc() throws Exception {
    parity(
        "DATE '1969-12-31', TIME '23:59:59', TIMESTAMP '1969-12-31 23:59:59.999999999', CAST(NULL"
            + " AS TIMESTAMP(9)), CAST(NULL AS DATE), INTERVAL '1' DAY, INTERVAL '1' MONTH, (ts,"
            + " INTERVAL '1' HOUR) OVERLAPS (TIMESTAMP '2024-03-10 01:00:00', INTERVAL '2' HOUR)");
  }

  @Test
  void temporalFunctionsWorkInFiltersWithTheSessionTimeZone() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalFunctionsSqlHarnessTest::environment,
        "SELECT id FROM temporal_inputs WHERE EXTRACT(HOUR FROM ltz) <> 1 "
            + "AND TO_TIMESTAMP(s) > TIMESTAMP '1900-01-01 00:00:00'");
  }

  @Test
  void roundedTimestampsCanBeGroupingKeys() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkTemporalFunctionsSqlHarnessTest::environment,
        "SELECT FLOOR(ts TO HOUR), COUNT(*) FROM temporal_inputs GROUP BY FLOOR(ts TO HOUR)");
  }

  @Test
  void temporalJoinPredicatesUseTheSessionTimeZone() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalFunctionsSqlHarnessTest::environment,
        "SELECT a.id, b.id FROM temporal_inputs a JOIN temporal_inputs b ON a.id = b.id "
            + "AND EXTRACT(HOUR FROM a.ltz) + 1 > EXTRACT(HOUR FROM b.ltz)");
  }

  @Test
  void parsedRowtimeCanFeedAWindow() throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var tables = StreamTableEnvironment.create(env);
          tables.createTemporaryView(
              "parsed",
              fromData(
                  env,
                  Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
                  Row.of("2024-01-01 00:00:00"),
                  Row.of("2024-01-01 00:00:01"),
                  Row.of("2024-01-01 00:00:03")),
              Schema.newBuilder()
                  .column("s", DataTypes.STRING())
                  .columnByExpression("rt", "TO_TIMESTAMP(s)")
                  .watermark("rt", "rt - INTERVAL '1' SECOND")
                  .build());
          return tables;
        },
        "SELECT window_start, window_end, COUNT(*) FROM "
            + "TABLE(TUMBLE(TABLE parsed, DESCRIPTOR(rt), INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void invalidTextAndPatternsRetainFlinksNullAndSentinelResults() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalFunctionsSqlHarnessTest::invalidDates,
        "SELECT TO_DATE(s, p), TO_TIMESTAMP(s, p), "
            + "UNIX_TIMESTAMP(s, p), CONVERT_TZ(s, z, 'UTC') FROM invalid_dates");
  }

  @Test
  void invalidLocalTimeZoneTimestampTextKeepsHostResults() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.TIMESTAMP_LTZ_TEXT_OVERLOADS,
        "Flink 1.18 TO_TIMESTAMP_LTZ has no string overloads");
    NativeParity.assertParity(
        FlinkTemporalFunctionsSqlHarnessTest::invalidDates,
        "SELECT TO_TIMESTAMP_LTZ(s, p, z) FROM invalid_dates");
  }

  private static TableEnvironment invalidDates() {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "invalid_dates",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"s", "p", "z"}, Types.STRING, Types.STRING, Types.STRING),
            Row.of("not a date", "yyyy-MM-dd HH:mm:ss", "UTC"),
            Row.of("2024-02-30 12:00:00", "yyyy-MM-dd HH:mm:ss", "unknown/timezone"),
            Row.of("2024-01-01 12:00:00", "yyyy/MM/dd", "UTC"),
            Row.of("", "", ""),
            Row.of(null, null, null)));
          return tables;
  }

  @Test
  void parsesLocalTimeZoneTimestampsFromText() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.TIMESTAMP_LTZ_TEXT_OVERLOADS,
        "Flink 1.18 TO_TIMESTAMP_LTZ only accepts numeric epochs with explicit precision");
    parity("TO_TIMESTAMP_LTZ(s), TO_TIMESTAMP_LTZ(s, p), TO_TIMESTAMP_LTZ(s, p, z)");
  }

  @Test
  void epochConversionsUseDefaultPrecision() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.TIMESTAMP_LTZ_DEFAULT_PRECISION,
        "Flink 1.18 TO_TIMESTAMP_LTZ requires the precision argument");
    parity("TO_TIMESTAMP_LTZ(epoch)");
  }

  @Test
  void castsPreEpochTimestampToTime() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NEGATIVE_TIMESTAMP_TO_TIME,
        "Flink 1.18 emits a negative millisecond fraction which its external TIME converter"
            + " rejects");
    parity("CAST(ts AS TIME)");
  }

  @Test
  void invalidFormatsFailLikeFlink() throws Exception {
    String sql = "SELECT TO_DATE(s, 'invalid!') FROM temporal_inputs";
    Throwable host = queryFailure(environment(), sql);
    var nativeEnvironment = environment();
    var scan = tech.streamfusion.planner.NativePlanner.install(nativeEnvironment);
    Throwable actual = queryFailure(nativeEnvironment, sql);
    org.junit.jupiter.api.Assertions.assertTrue(scan.substitutions() > 0);
    org.junit.jupiter.api.Assertions.assertEquals(host.getClass(), actual.getClass());
    org.junit.jupiter.api.Assertions.assertEquals(host.getMessage(), actual.getMessage());
  }

  private static Throwable queryFailure(TableEnvironment tables, String sql) {
    Throwable failure =
        org.junit.jupiter.api.Assertions.assertThrows(
            Exception.class,
            () -> {
              try (var rows = tables.executeSql(sql).collect()) {
                while (rows.hasNext()) {
                  rows.next();
                }
              }
            });
    while (failure.getCause() != null) {
      failure = failure.getCause();
    }
    return failure;
  }

  private static void parity(String expressions) throws Exception {
    NativeParity.assertParity(
        FlinkTemporalFunctionsSqlHarnessTest::environment,
        "SELECT id, " + expressions + " FROM temporal_inputs");
  }

  static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    tables.getConfig().setLocalTimeZone(ZoneId.of("America/New_York"));
    String[] timestamps = {
      "1969-12-31T23:59:59.999999999",
      "2000-02-29T12:34:56.123456789",
      "2024-03-10T02:30:00",
      "2024-11-03T01:30:00",
      "2024-01-31T23:59:59.999999999"
    };
    Row[] rows = new Row[timestamps.length + 1];
    for (int i = 0; i < timestamps.length; i++) {
      LocalDateTime ts = LocalDateTime.parse(timestamps[i]);
      Instant instant = ts.toInstant(java.time.ZoneOffset.UTC);
      String text = ts.withNano(0).toString().replace('T', ' ');
      if (text.length() == 16) {
        text += ":00";
      }
      rows[i] =
          Row.of(
              i,
              text,
              "yyyy-MM-dd HH:mm:ss",
              "America/New_York",
              i - 2,
              instant.toEpochMilli(),
              ts.toLocalDate(),
              ts.toLocalTime(),
              ts,
              instant);
    }
    rows[timestamps.length] =
        Row.of(
            timestamps.length,
            null,
            null,
            null,
            null,
            null,
            (LocalDate) null,
            (LocalTime) null,
            (LocalDateTime) null,
            (Instant) null);
    tables.createTemporaryView(
        "temporal_inputs",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "s", "p", "z", "n", "epoch", "d", "tm", "ts", "ltz"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.INT,
                Types.LONG,
                Types.LOCAL_DATE,
                Types.LOCAL_TIME,
                Types.LOCAL_DATE_TIME,
                Types.INSTANT),
            rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("p", DataTypes.STRING())
            .column("z", DataTypes.STRING())
            .column("n", DataTypes.INT())
            .column("epoch", DataTypes.BIGINT())
            .column("d", DataTypes.DATE())
            .column("tm", DataTypes.TIME(3))
            .column("ts", DataTypes.TIMESTAMP(9))
            .column("ltz", DataTypes.TIMESTAMP_LTZ(9))
            .build());
    return tables;
  }
}
