package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkMixedTimestampComparisonSqlHarnessTest {
  static Stream<Arguments> precisionsAndZones() {
    return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
        .flatMap(
            zone ->
                Stream.of(0, 3, 6, 9)
                    .flatMap(
                        plain ->
                            Stream.of(0, 3, 6, 9)
                                .map(instant -> Arguments.of(plain, instant, zone))));
  }

  @ParameterizedTest
  @MethodSource("precisionsAndZones")
  void comparisonsUseReleasedFlinkCoercionWithNullableAndDstInputs(
      int plainPrecision, int instantPrecision, String zone) throws Exception {
    List<String> expressions = new ArrayList<>();
    List<String> explicitCasts = new ArrayList<>();
    for (boolean reversed : new boolean[] {false, true}) {
      String left = reversed ? "b" : "a";
      String right = reversed ? "a" : "b";
      int leftPrecision = reversed ? instantPrecision : plainPrecision;
      int rightPrecision = reversed ? plainPrecision : instantPrecision;
      boolean castRight = leftPrecision > rightPrecision;
      String target = (castRight ? reversed : !reversed) ? "TIMESTAMP_LTZ" : "TIMESTAMP";
      target += "(" + Math.max(leftPrecision, rightPrecision) + ")";
      String castLeft = castRight ? left : "CAST(" + left + " AS " + target + ")";
      String castRightSql = castRight ? "CAST(" + right + " AS " + target + ")" : right;
      for (String operator :
          List.of("=", "<>", "<", "<=", ">", ">=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM")) {
        expressions.add(left + " " + operator + " " + right);
        // SQL validation already coerces equality to the left operand's type. The
        // compatibility rule must preserve that existing cast, including precision loss.
        if (operator.equals("=") || operator.equals("<>")) {
          String leftType = reversed ? "TIMESTAMP_LTZ" : "TIMESTAMP";
          explicitCasts.add(
              left
                  + " "
                  + operator
                  + " CAST("
                  + right
                  + " AS "
                  + leftType
                  + "("
                  + leftPrecision
                  + "))");
        } else if (reversed && leftPrecision == rightPrecision) {
          // Calcite shares the reversed ordering/distinct comparison with its forward
          // equivalent. At a DST overlap, the direction of the timezone cast matters.
          explicitCasts.add(
              left
                  + " "
                  + operator
                  + " CAST("
                  + right
                  + " AS TIMESTAMP_LTZ("
                  + leftPrecision
                  + "))");
        } else {
          explicitCasts.add(castLeft + " " + operator + " " + castRightSql);
        }
      }
    }
    String query = "SELECT " + String.join(", ", expressions) + " FROM src";
    String oracle = "SELECT " + String.join(", ", explicitCasts) + " FROM src";
    // The released 2.2 planner also changed SQL equality validation. Use its original
    // SQL as the oracle; on 1.18, retain that line's validation casts and explicitly
    // supply only the comparison casts its code generator cannot produce.
    String oracleSql =
        tech.streamfusion.compat.FlinkTestCapabilities.MIXED_TIMESTAMP_COMPARISONS ? query : oracle;
    var expected = collect(environment(plainPrecision, instantPrecision, zone), oracleSql);
    for (boolean nativeRun : new boolean[] {false, true}) {
      var table = environment(plainPrecision, instantPrecision, zone);
      table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeRun));
      var scan = NativePlanner.install(table);
      var actual = collect(table, query);
      assertEquals(
          NativeParity.multiset(expected),
          NativeParity.multiset(actual),
          query
              + " / "
              + zone
              + " / native="
              + nativeRun
              + " / precision="
              + plainPrecision
              + ","
              + instantPrecision);
      if (nativeRun) assertTrue(scan.substitutions() > 0, scan::explainSummary);
      else assertEquals(0, scan.substitutions());
    }
  }

  private static TableEnvironment environment(
      int plainPrecision, int instantPrecision, String zone) {
    LocalDateTime local =
        LocalDateTime.of(2021, 3, 14, 1, 59, 59).withNano(fraction(plainPrecision));
    Instant instant =
        local.withNano(fraction(instantPrecision)).atZone(ZoneId.of(zone)).toInstant();
    LocalDateTime ambiguous = LocalDateTime.of(2021, 11, 7, 1, 30);
    return BuiltinFunctionParity.environment(
        ROW(FIELD("a", TIMESTAMP(plainPrecision)), FIELD("b", TIMESTAMP_LTZ(instantPrecision))),
        List.of(
            Row.of(local, instant),
            Row.of(local, instant.plusSeconds(1)),
            Row.of(local, instant.minusSeconds(1)),
            Row.of(null, instant),
            Row.of(local, null),
            Row.of(null, null),
            Row.of(ambiguous, Instant.parse("2021-11-07T08:30:00Z")),
            Row.of(ambiguous, Instant.parse("2021-11-07T09:30:00Z"))),
        zone);
  }

  private static int fraction(int precision) {
    int scale = (int) Math.pow(10, 9 - precision);
    return 123456789 / scale * scale;
  }

  private static List<List<Object>> collect(TableEnvironment table, String sql) throws Exception {
    List<List<Object>> result = new ArrayList<>();
    try (var rows = table.executeSql(sql).collect()) {
      while (rows.hasNext()) {
        Row row = rows.next();
        Object[] values = new Object[row.getArity()];
        for (int i = 0; i < values.length; i++) values[i] = row.getField(i);
        result.add(Arrays.asList(values));
      }
    }
    return result;
  }
}
