package tech.streamfusion;

import java.time.ZoneId;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkFromUnixTimeSqlHarnessTest {
  private static final String PARTITIONS =
      "SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 0, 8) AS ds, "
          + "SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 9, 2) AS hh, "
          + "LPAD(CAST((CAST(SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 11, 2) AS INT) "
          + "/ 15) * 15 AS STRING), 2, '0') AS mm";
  private static final long[] EDGES = {
    0,
    -1,
    Long.MIN_VALUE,
    Long.MAX_VALUE,
    -12219292800L,
    -12219292801L,
    -62135769600L,
    -62135769601L,
    Long.MAX_VALUE / 1000,
    Long.MIN_VALUE / 1000,
    Long.MAX_VALUE / 1000 + 1,
    Long.MIN_VALUE / 1000 - 1,
    1710053999L,
    1710054000L,
    1730613599L,
    1730613600L,
    Integer.MIN_VALUE,
    Integer.MAX_VALUE,
    2114380800L - 7200 - 1,
    2114380800L - 7200,
    2114380800L - 3600,
    4102444800L - 7200 - 1,
    4102444800L - 7200,
    4102444800L - 3600 - 1,
    4102444800L - 3600,
    4102444800L
  };

  @ParameterizedTest
  @ValueSource(
      strings = {
        "UTC",
        "GMT+05:30",
        "GMT-12:00",
        "GMT+18:00",
        "Etc/GMT+8",
        "Asia/Shanghai",
        "Asia/Kolkata",
        "Africa/Windhoek",
        "Pacific/Apia",
        "America/New_York",
        "Europe/Paris"
      })
  void fullRangeAndOverflowMatchFlinkInEveryAdmittedZone(String zone) throws Exception {
    NativeParity.assertParity(
        () -> environment(zone),
        "SELECT FROM_UNIXTIME(n), FROM_UNIXTIME(n, 'yyyyMMddHHmm'), "
            + "FROM_UNIXTIME(n, 'yyyy-MM-dd''T''HH:mm:ss''|😀'''), "
            + "FROM_UNIXTIME(i, 'yyyyMMddHHmm'), FROM_UNIXTIME(n, '') FROM src");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void datePartitionsUseOnlyNativeFunctions(boolean legacy) {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = environment("Asia/Shanghai");
              table
                  .getConfig()
                  .set("table.exec.legacy-cast-behaviour", legacy ? "enabled" : "disabled");
              return table;
            },
            "SELECT n, " + PARTITIONS + " FROM src");
    assertEntirelyNative(comparison);
  }

  @Test
  void wideDatePartitionsPreserveEveryPassthroughFieldAcrossBatches() {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var fields = new java.util.ArrayList<org.apache.flink.table.api.DataTypes.Field>();
              fields.add(
                  org.apache.flink.table.api.DataTypes.FIELD(
                      "n", org.apache.flink.table.api.DataTypes.BIGINT()));
              for (int column = 0; column < 107; column++)
                fields.add(
                    org.apache.flink.table.api.DataTypes.FIELD(
                        "c" + column, org.apache.flink.table.api.DataTypes.STRING()));
              var rows = new java.util.ArrayList<Row>();
              for (int id = 0; id < 3075; id++) {
                var row = Row.withPositions(108);
                row.setField(0, id % 19 == 0 ? null : 1730000000L + id);
                for (int column = 0; column < 107; column++)
                  if ((id + column) % 23 != 0)
                    row.setField(column + 1, "field-" + column + "-value-" + id);
                rows.add(row);
              }
              var table =
                  BuiltinFunctionParity.environment(
                      org.apache.flink.table.api.DataTypes.ROW(
                          fields.toArray(new org.apache.flink.table.api.DataTypes.Field[0])),
                      rows);
              table.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
              table.getConfig().set("table.exec.legacy-cast-behaviour", "enabled");
              return table;
            },
            "SELECT *, " + PARTITIONS + " FROM src");
    assertEntirelyNative(comparison);
  }

  private static void assertEntirelyNative(NativeFailureParity.Comparison comparison) {
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    org.junit.jupiter.api.Assertions.assertTrue(comparison.nativeRun().fallbackReasons().isEmpty());
    String plan = comparison.nativeRun().plan();
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("jvmFunctions=[]"), plan);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("nativeFunctions=["), plan);
  }

  @Test
  void dynamicUnsupportedAndNullPatternsRemainHostExact() throws Exception {
    NativeParity.assertParity(
        () -> environment("UTC"),
        "SELECT FROM_UNIXTIME(n, p), FROM_UNIXTIME(n, 'EEEE MMMM dd yyyy'), "
            + "FROM_UNIXTIME(n, CAST(NULL AS STRING)), "
            + "CHAR_LENGTH(FROM_UNIXTIME(n, 'yyyyMMddHHmm')) FROM src");
  }

  @Test
  void nativeFormattingFeedsHostTemporalConsumers() throws Exception {
    NativeParity.assertParity(
        () -> environment("UTC"),
        "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(i)), FROM_UNIXTIME(n, 'yyyyMMddHHmm') IS NULL "
            + "FROM src");
  }

  @Test
  void invalidPatternKeepsFlinksFailure() {
    NativeFailureParity.run(() -> environment("UTC"), "SELECT FROM_UNIXTIME(n, 'yyyy-QQ') FROM src")
        .assertFailure(
            IllegalArgumentException.class,
            "Illegal pattern character 'Q'",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(String zone) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.configure(
        org.apache.flink.configuration.Configuration.fromMap(
            java.util.Map.of("restart-strategy.type", "none")));
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    long[] samples =
        java.util.stream.LongStream.concat(
                java.util.Arrays.stream(EDGES),
                ZoneId.of(zone).getRules().getTransitions().stream()
                    .flatMapToLong(
                        transition -> {
                          long seconds = transition.getInstant().getEpochSecond();
                          return java.util.stream.LongStream.of(seconds - 1, seconds, seconds + 1);
                        }))
            .toArray();
    table.createTemporaryView(
        "src",
        env.fromSequence(0, 5002)
            .map(
                id -> {
                  Long seconds;
                  if (id < samples.length) seconds = samples[id.intValue()];
                  else if (id % 8 == 0) seconds = null;
                  else seconds = new java.util.Random(id * 31 + 123).nextLong();
                  return Row.of(
                      seconds,
                      seconds == null ? null : seconds.intValue(),
                      id % 11 == 0 ? null : id % 2 == 0 ? "yyyyMMddHHmm" : "yyyy-MM-dd HH:mm:ss");
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"n", "i", "p"}, Types.LONG, Types.INT, Types.STRING)));
    return table;
  }
}
