package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkIntegralTruncatePositionSqlHarnessTest {
  private static final int[] POSITIONS = {-38, -20, -19, -18, -10, -3, -2, -1, 0, 1, 18, 38};

  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void towardZeroAndAllVerifiedPositionsMatchReleasedFlink(String type) throws Exception {
    String expressions =
        IntStream.rangeClosed(-38, 38)
            .mapToObj(p -> "TRUNCATE(v," + p + ")")
            .collect(Collectors.joining(","));
    BuiltinFunctionParity.assertParity(
        () -> environment(type, true),
        "SELECT " + expressions + ", TRUNCATE(v,CAST(NULL AS INT)) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(type, false), "SELECT " + expressions + " FROM src WHERE MOD(id,2)=0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type, true),
        "SELECT TRUNCATE(TRUNCATE(v,-1),-2), TRUNCATE(CAST(v AS BIGINT),-19),"
            + " FLOOR(TRUNCATE(v,-1)), CAST(TRUNCATE(v,-1) AS STRING),"
            + " ROUND(TRUNCATE(v,-1),-2), TRUNCATE(ROUND(v,-1),-2) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(type, true), "SELECT id FROM src WHERE TRUNCATE(v,-1)>0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void expectedKernelConsumesRowsWithoutJvmEvaluation(String type) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
      DataType inputType = inputType(type, true);
      List<Row> input = rows(type, 5003, true);
      table.createTemporaryView(
          "src",
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env,
              input,
              org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(inputType)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(inputType).build());
      var scan = NativePlanner.install(table);
      String sql =
          "SELECT "
              + IntStream.of(POSITIONS)
                  .mapToObj(p -> "TRUNCATE(v," + p + ")")
                  .collect(Collectors.joining(","))
              + ", TRUNCATE(v,CAST(NULL AS INT)) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("TRUNCATE") && route.contains("jvm=[]")),
          scan.explainSummary());
      int count = 0;
      try (var results = table.executeSql(sql).collect()) {
        while (results.hasNext()) {
          Row actual = results.next();
          Number value = (Number) input.get(count).getField(1);
          for (int column = 0; column < POSITIONS.length; column++) {
            Number expected =
                value == null
                    ? null
                    : narrow(
                        BigDecimal.valueOf(value.longValue())
                            .setScale(POSITIONS[column], java.math.RoundingMode.DOWN)
                            .longValue(),
                        type);
            assertEquals(expected, actual.getField(column));
          }
          assertNull(actual.getField(POSITIONS.length));
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @Test
  void failedArgumentsRemainLazyAndEmptyFilteredBatchesStayEmpty() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT", true), "SELECT TRUNCATE(v/(id-id),-1) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT", true),
        "SELECT CASE WHEN id<0 THEN TRUNCATE(v/(id-id),-1) ELSE TRUNCATE(v,-1) END FROM src");
    NativeFailureParity.run(
            () -> environment("BIGINT", true), "SELECT TRUNCATE(v/(id-id),-1) FROM src")
        .assertFailure(
            ArithmeticException.class,
            "zero",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT", true), "SELECT TRUNCATE(v,2147483647) FROM src");
    for (int position : new int[] {Integer.MIN_VALUE}) {
      BuiltinFunctionParity.assertParity(
          () -> environment("BIGINT", true),
          "SELECT TRUNCATE(v," + position + ") FROM src WHERE id<0");
      var runs =
          NativeFailureParity.run(
              () -> environment("BIGINT", true),
              "SELECT TRUNCATE(v," + position + ") FROM src WHERE id=1");
      runs.assertFailure(
          ArithmeticException.class,
          "Underflow",
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT"})
  void narrowInputsRequireExplicitWidening(String type) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(type, true),
        "SELECT TRUNCATE(CAST(v AS INT),-1), TRUNCATE(CAST(v AS BIGINT),-19) FROM src");
    var runs =
        NativeFailureParity.run(() -> environment(type, true), "SELECT TRUNCATE(v,-1) FROM src");
    runs.assertFailure(
        org.codehaus.commons.compiler.CompileException.class,
        "Assignment conversion not possible",
        runs.host().phase(),
        NativeFailureParity.Route.FALLBACK);
  }

  private static TableEnvironment environment(String type, boolean nullable) {
    return BuiltinFunctionParity.environment(inputType(type, nullable), rows(type, 41, nullable));
  }

  private static DataType inputType(String type, boolean nullable) {
    DataType value;
    switch (type) {
      case "TINYINT":
        value = TINYINT();
        break;
      case "SMALLINT":
        value = SMALLINT();
        break;
      case "INT":
        value = INT();
        break;
      case "BIGINT":
        value = BIGINT();
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    return ROW(FIELD("id", INT().notNull()), FIELD("v", nullable ? value : value.notNull()));
  }

  private static List<Row> rows(String type, int count, boolean nullable) {
    long[] values = {
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      -9007199254740993L,
      9007199254740993L,
      -155,
      -150,
      -149,
      -15,
      -1,
      0,
      1,
      15,
      149,
      150,
      155,
      Integer.MIN_VALUE,
      Integer.MAX_VALUE
    };
    var rows = new ArrayList<Row>();
    for (int i = 0; i < count; i++)
      rows.add(Row.of(i, nullable && i % 7 == 0 ? null : narrow(values[i % values.length], type)));
    return rows;
  }

  private static Number narrow(long value, String type) {
    switch (type) {
      case "TINYINT":
        return (byte) value;
      case "SMALLINT":
        return (short) value;
      case "INT":
        return (int) value;
      case "BIGINT":
        return value;
      default:
        throw new IllegalArgumentException(type);
    }
  }
}
