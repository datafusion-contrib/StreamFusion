package tech.streamfusion;

import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkIntervalExprSqlHarnessTest {

  @Test
  void dayTimeIntervalLiteralMatchesHost() throws Exception {
    NativeParity.assertParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT k, INTERVAL '1' DAY AS d FROM t");
  }

  @Test
  void subSecondIntervalLiteralMatchesHost() throws Exception {
    NativeParity.assertParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT k, INTERVAL '1.250' SECOND AS d FROM t");
  }

  @Test
  void intervalLiteralChosenPerRowMatchesHost() throws Exception {
    // CASE prevents constant folding and exercises both signs in the interval array.
    NativeParity.assertParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT k, CASE WHEN k > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END AS d FROM t");
  }

  @Test
  void intervalLiteralGroupKeyMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L),
                  Row.of((Object) null),
                  Row.of((Object) null));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT d, COUNT(*) FROM (SELECT CASE WHEN k > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END d FROM t) GROUP BY d");
  }

  @Test
  void nullableIntervalLiteralGroupKeyMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L),
                  Row.of((Object) null),
                  Row.of((Object) null));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT d, COUNT(*) FROM (SELECT CASE WHEN k IS NULL THEN NULL "
            + "WHEN k > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END d FROM t) GROUP BY d");
  }

  @Test
  void intervalComparisonAfterTopNFallsBackBeforeExecution() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT k, d FROM (SELECT k, CASE WHEN k > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END AS d FROM t ORDER BY d, k LIMIT 4) "
            + "WHERE d > INTERVAL '0' SECOND",
        "condition does not compile natively");
  }

  @Test
  void intervalOuterJoinResidualFallsBackBeforeExecution() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> left =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
                  Row.of(1L, 1L), Row.of(2L, 3L), Row.of(3L, 1L), Row.of(4L, null));
          DataStream<Row> right =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
                  Row.of(1L, 1L), Row.of(2L, 3L), Row.of(4L, null), Row.of(5L, 3L));
          tEnv.createTemporaryView(
              "a", left,
              Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
          tEnv.createTemporaryView(
              "b", right,
              Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT l.k, l.d, r.k, r.d FROM "
            + "(SELECT k, CASE WHEN v IS NULL THEN NULL WHEN v > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END AS d FROM a ORDER BY k LIMIT 4) l "
            + "LEFT JOIN "
            + "(SELECT k, CASE WHEN v IS NULL THEN NULL WHEN v > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END AS d FROM b ORDER BY k LIMIT 4) r "
            + "ON l.k = r.k AND (l.d + r.d) > INTERVAL '0' SECOND",
        "residual condition does not compile natively");
  }

  @Test
  void intervalOuterJoinColumnResidualMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> left =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
                  Row.of(1L, 1L), Row.of(2L, 3L), Row.of(3L, 1L), Row.of(4L, null));
          DataStream<Row> right =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
                  Row.of(1L, 1L), Row.of(2L, 3L), Row.of(4L, null), Row.of(5L, 3L));
          tEnv.createTemporaryView(
              "a", left,
              Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
          tEnv.createTemporaryView(
              "b", right,
              Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT l.k, l.d, r.k, r.d FROM "
            + "(SELECT k, CASE WHEN v IS NULL THEN NULL WHEN v > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END AS d FROM a ORDER BY k LIMIT 4) l "
            + "LEFT JOIN "
            + "(SELECT k, CASE WHEN v IS NULL THEN NULL WHEN v > 2 THEN INTERVAL '1' DAY "
            + "ELSE INTERVAL -'2.250' SECOND END AS d FROM b ORDER BY k LIMIT 4) r "
            + "ON l.k = r.k AND (l.d + r.d) > (l.d - r.d)");
  }

  @Test
  void nanosecondTimestampDifferenceFallsBack() throws Exception {
    // Flink floors each operand to milliseconds before subtraction, not the finished duration.
    NativeParity.assertFallbackReasonContains(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(
                      new String[] {"k", "a", "b"},
                      Types.LONG,
                      Types.LOCAL_DATE_TIME,
                      Types.LOCAL_DATE_TIME),
                  Row.of(
                      1L,
                      LocalDateTime.parse("1970-01-01T00:00:00.001400000"),
                      LocalDateTime.parse("1970-01-01T00:00:00.000500000")),
                  Row.of(
                      2L,
                      LocalDateTime.parse("1970-01-01T00:00:00.000500000"),
                      LocalDateTime.parse("1970-01-01T00:00:00.001400000")),
                  Row.of(3L, null, LocalDateTime.parse("1970-01-01T00:00:00.000500000")),
                  Row.of(4L, LocalDateTime.parse("1970-01-01T00:00:00.001400000"), null),
                  Row.of(5L, null, null),
                  Row.of(
                      6L,
                      LocalDateTime.parse("1969-12-31T23:59:59.999500000"),
                      LocalDateTime.parse("1969-12-31T23:59:59.998600000")),
                  Row.of(
                      7L,
                      LocalDateTime.parse("1969-12-31T23:59:59.998600000"),
                      LocalDateTime.parse("1969-12-31T23:59:59.999500000")));
          tEnv.createTemporaryView(
              "t",
              source,
              Schema.newBuilder()
                  .column("k", DataTypes.BIGINT())
                  .column("a", DataTypes.TIMESTAMP(9))
                  .column("b", DataTypes.TIMESTAMP(9))
                  .build());
          return tEnv;
        },
        "SELECT k, (a - b) SECOND AS d FROM t",
        "Duration(NANOSECOND)");
  }

  @Test
  void yearMonthIntervalChosenPerRowFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          DataStream<Row> source =
              env.fromData(
                  Types.ROW_NAMED(new String[] {"k"}, Types.LONG),
                  Row.of(1L),
                  Row.of(2L),
                  Row.of(3L),
                  Row.of(4L),
                  Row.of(5L),
                  Row.of((Object) null));
          tEnv.createTemporaryView(
              "t", source, Schema.newBuilder().column("k", DataTypes.BIGINT()).build());
          return tEnv;
        },
        "SELECT k, CASE WHEN k > 2 THEN INTERVAL '2' YEAR "
            + "ELSE INTERVAL -'3' MONTH END AS d FROM t",
        "INTERVAL_YEAR_MONTH");
  }
}
