package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkTimestampExtractSqlHarnessTest {
  @Test
  void extractedYearFeedsNativeGrouping() throws Exception {
    NativeParity.assertParity(
        () -> environment(9),
        "SELECT EXTRACT(YEAR FROM ts), COUNT(*) FROM timestamps GROUP BY EXTRACT(YEAR FROM ts)");
  }

  @Test
  void extractedYearFiltersRows() throws Exception {
    NativeParity.assertParity(
        () -> environment(9), "SELECT id FROM timestamps WHERE EXTRACT(YEAR FROM ts) = 1970");
  }

  @Test
  void extractedYearPreservesCaseNullHandling() throws Exception {
    NativeParity.assertParity(
        () -> environment(9),
        "SELECT id, CASE WHEN ts IS NULL THEN -1 ELSE EXTRACT(YEAR FROM ts) END FROM timestamps");
  }

  @ParameterizedTest(name = "{0}, precision {1}")
  @MethodSource("fields")
  void timestampFieldMatchesFlinkBeforeEpoch(String field, int precision) throws Exception {
    NativeParity.assertParity(
        () -> environment(precision), "SELECT id, EXTRACT(" + field + " FROM ts) FROM timestamps");
  }

  @ParameterizedTest(name = "LTZ {0}, precision {1}")
  @MethodSource("fields")
  void ltzFieldMatchesFlinkBeforeEpoch(String field, int precision) throws Exception {
    NativeParity.assertParity(
        () -> environment(precision), "SELECT id, EXTRACT(" + field + " FROM ltz) FROM timestamps");
  }

  static Stream<Arguments> fields() {
    return Stream.of("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND")
        .flatMap(field -> Stream.of(0, 3, 6, 9).map(precision -> Arguments.of(field, precision)));
  }

  private static TableEnvironment environment(int precision) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    Row[] rows =
        Stream.of(
                "1969-12-31T23:59:59.999999999",
                "1969-12-31T23:59:59.999999",
                "1969-12-31T23:59:59.999",
                "1969-12-31T23:59:59",
                "1969-12-31T12:34:56.789",
                "1969-12-31T00:00:00",
                "1969-12-30T23:59:59.999",
                "1900-02-28T23:59:59.999",
                "1970-01-01T00:00:00",
                "1970-01-01T12:34:56.789",
                "2024-02-29T23:59:59.999")
            .map(LocalDateTime::parse)
            .map(value -> Row.of(value.toString(), value, value.toInstant(ZoneOffset.UTC)))
            .toArray(Row[]::new);
    Row[] withNull = java.util.Arrays.copyOf(rows, rows.length + 1);
    withNull[rows.length] = Row.of("null", (LocalDateTime) null, (Instant) null);
    tables.createTemporaryView(
        "timestamps",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "ts", "ltz"},
                Types.STRING,
                Types.LOCAL_DATE_TIME,
                Types.INSTANT),
            withNull),
        Schema.newBuilder()
            .column("id", DataTypes.STRING())
            .column("ts", DataTypes.TIMESTAMP(precision))
            .column("ltz", DataTypes.TIMESTAMP_LTZ(precision))
            .build());
    return tables;
  }
}
