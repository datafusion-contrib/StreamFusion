package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkTimestampExtremaSqlHarnessTest {
  static Stream<Arguments> timestampModes() {
    return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
        .flatMap(zone -> Stream.of(0, 3, 6, 9).map(precision -> Arguments.of(zone, precision)));
  }

  @ParameterizedTest
  @MethodSource("timestampModes")
  void twoPhaseExtremaPreservePrecisionAndSessionZone(String zone, int precision) throws Exception {
    NativeParity.assertChangelogParity(
        () -> environment(zone, precision),
        "SELECT k, MIN(ts), MAX(ts), MIN(ltz), MAX(ltz), COUNT(*) FROM src GROUP BY k");
  }

  @Test
  void filtersAndGlobalExtremaPreserveNullResults() throws Exception {
    NativeParity.assertChangelogParity(
        () -> environment("Asia/Shanghai", 9),
        "SELECT MIN(ts), MAX(ts), MIN(ltz) FILTER (WHERE k = 3), "
            + "MAX(ltz) FILTER (WHERE k = 2), COUNT(ts) FROM src");
  }

  @ParameterizedTest
  @MethodSource("timestampModes")
  void singlePhaseExtremaMatchHost(String zone, int precision) throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment table = environment(zone, precision);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          table.getConfig().set("table.exec.mini-batch.enabled", "false");
          return table;
        },
        "SELECT k, MIN(ts), MAX(ltz) FROM src GROUP BY k");
  }

  @Test
  void retractingExtremaPreserveMultiplicityAndRemoveEmptyGroups() throws Exception {
    NativeParity.assertKindedParity(
        FlinkTimestampExtremaSqlHarnessTest::retractingEnvironment,
        "SELECT k, MIN(ts), MAX(ts), MIN(ltz), MAX(ltz) FROM src GROUP BY k");
  }

  private static TableEnvironment retractingEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "America/Los_Angeles");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    LocalDateTime low = LocalDateTime.parse("1969-12-31T23:59:59.999000001");
    LocalDateTime high = LocalDateTime.parse("1969-12-31T23:59:59.999999999");
    List<Row> rows =
        List.of(
            Row.ofKind(RowKind.INSERT, 1, low, low.toInstant(ZoneOffset.UTC)),
            Row.ofKind(RowKind.INSERT, 1, low, low.toInstant(ZoneOffset.UTC)),
            Row.ofKind(RowKind.INSERT, 1, high, high.toInstant(ZoneOffset.UTC)),
            Row.ofKind(RowKind.INSERT, 2, null, null),
            Row.ofKind(RowKind.DELETE, 1, low, low.toInstant(ZoneOffset.UTC)),
            Row.ofKind(RowKind.DELETE, 1, low, low.toInstant(ZoneOffset.UTC)),
            Row.ofKind(RowKind.DELETE, 1, high, high.toInstant(ZoneOffset.UTC)),
            Row.ofKind(RowKind.DELETE, 2, null, null));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            fromData(
                env,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "ts", "ltz"},
                    Types.INT,
                    Types.LOCAL_DATE_TIME,
                    Types.INSTANT)),
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("ts", DataTypes.TIMESTAMP(9))
                .column("ltz", DataTypes.TIMESTAMP_LTZ(9))
                .build()));
    return table;
  }

  private static TableEnvironment environment(String zone, int precision) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", zone);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "2");
    String[] samples = {
      "1969-12-31T23:59:59.999999999",
      "1969-12-31T23:59:59.999000001",
      "0001-01-01T00:00:00.123456789",
      "9999-12-31T23:59:59.999999999",
      "2000-02-29T12:34:56.123456789",
      "2000-02-29T12:34:56.123456789",
      null
    };
    List<Row> rows = new ArrayList<>();
    for (int key = 0; key < 4; key++) {
      for (String sample : samples) {
        LocalDateTime ts = sample == null || key == 3 ? null : LocalDateTime.parse(sample);
        if (ts != null) {
          int quantum = (int) Math.pow(10, 9 - precision);
          ts = ts.withNano(ts.getNano() / quantum * quantum);
        }
        Instant instant = ts == null ? null : ts.toInstant(ZoneOffset.UTC);
        rows.add(Row.of(key, ts, instant));
      }
    }
    table.createTemporaryView(
        "src",
        fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"k", "ts", "ltz"}, Types.INT, Types.LOCAL_DATE_TIME, Types.INSTANT)),
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("ts", DataTypes.TIMESTAMP(precision))
            .column("ltz", DataTypes.TIMESTAMP_LTZ(precision))
            .build());
    return table;
  }
}
