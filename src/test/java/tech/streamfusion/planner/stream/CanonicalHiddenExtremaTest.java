package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalHiddenExtremaTest {
  static Stream<Arguments> lifecycleCases() {
    return Stream.of(false, true)
        .flatMap(
            append ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                first ->
                                                    Stream.of(false, true)
                                                        .flatMap(
                                                            before ->
                                                                Stream.of("bigint", "int", "global")
                                                                    .map(
                                                                        key ->
                                                                            Arguments.of(
                                                                                append, rocks, ttl,
                                                                                first, before,
                                                                                key)))))));
  }

  @ParameterizedTest
  @MethodSource("lifecycleCases")
  void hiddenSumAndAverageCountsKeepExtremaViewsAtTheirActualAggregateIds(
      boolean append, boolean rocks, long ttl, boolean first, boolean before, String key)
      throws Exception {
    boolean global = key.equals("global"), integer = key.equals("int");
    Long[] values = {null, 11L, 5L, 7L, 4L, 11L, 7L, 4L, 5L, null, 17L, null};
    int[] groups = {1, 2, 0, 1, 1, 2, 1, 1, 0, 1, 1, 2};
    int[] kinds = {0, 0, 0, 0, 0, 3, 1, 3, 3, 3, 0, 0};
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < values.length; i++) {
      Object group =
          groups[i] == 0
              ? null
              : integer
                  ? (Object) (groups[i] == 1 ? Integer.MIN_VALUE : Integer.MAX_VALUE)
                  : (Object) (groups[i] == 1 ? Long.MIN_VALUE : Long.MAX_VALUE);
      var row = global ? GenericRowData.of(values[i]) : GenericRowData.of(group, values[i]);
      row.setRowKind(append ? RowKind.INSERT : RowKind.fromByteValue((byte) kinds[i]));
      rows.add(row);
    }
    CanonicalGroupArithmeticInitializationTest.assertLifecycle(
        "hidden-extrema", rocks, ttl, first, before, append, integer, global, rows);
  }

  static Stream<Arguments> planCases() {
    return Stream.of(false, true)
        .flatMap(
            append ->
                Stream.of("bigint", "int", "global")
                    .flatMap(
                        key ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of(0L, 1000L)
                                            .flatMap(
                                                ttl ->
                                                    Stream.of("DISABLED", "ALWAYS")
                                                        .map(
                                                            uids ->
                                                                Arguments.of(
                                                                    append, key, compiled, ttl,
                                                                    uids))))));
  }

  @ParameterizedTest
  @MethodSource("planCases")
  void generatedPhysicalOffsetsAndMapIdsSurviveBothSubmissionForms(
      boolean append, String key, boolean compiled, long ttl, String uids) throws Exception {
    new CanonicalGroupArithmeticPlanTest()
        .assertPlan(
            "hidden-extrema", compiled, ttl, uids, append, key.equals("int"), key.equals("global"));
  }
}
