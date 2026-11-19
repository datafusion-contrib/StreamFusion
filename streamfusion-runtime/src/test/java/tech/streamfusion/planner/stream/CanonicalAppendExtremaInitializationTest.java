package tech.streamfusion.planner.stream;

import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalAppendExtremaInitializationTest {
  static Stream<Arguments> cases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
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
                                                                                mode, rocks, ttl,
                                                                                first, before,
                                                                                key)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void nullableExtremaAndRealCountersKeepTheActualStockState(
      String mode, boolean rocks, long ttl, boolean first, boolean before, String key)
      throws Exception {
    boolean global = key.equals("global"), integer = key.equals("int");
    var values =
        new Long[] {
          null, 11L, 5L, 7L, 4L, -11L, -7L, -4L, Long.MIN_VALUE, null, Long.MAX_VALUE, null
        };
    var steps = new java.util.ArrayList<org.apache.flink.table.data.RowData>();
    for (int i = 0; i < values.length; i++) {
      Object group =
          integer
              ? (Object) (i % 3 == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE)
              : (Object) (i % 3 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE);
      if (i == 2 || i == 8) group = null;
      steps.add(global ? GenericRowData.of(values[i]) : GenericRowData.of(group, values[i]));
    }
    assertLifecycle(mode, rocks, ttl, first, before, true, integer, global, steps);
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .map(
                                                global ->
                                                    Arguments.of(mode, rocks, ttl, global)))));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void nativeCheckpointsPreserveInitializedAndNullableExtrema(
      String mode, boolean rocks, long ttl, boolean global) throws Exception {
    CanonicalGroupArithmeticRecoveryTest.assertNativeRecovery(mode, rocks, ttl, true, global);
  }

  static Stream<Arguments> emptyCases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of("bigint", "int", "global")
                                            .map(key -> Arguments.of(mode, rocks, ttl, key)))));
  }

  @ParameterizedTest
  @MethodSource("emptyCases")
  void emptyExtremaDoNotBecomeInitialized(String mode, boolean rocks, long ttl, String key)
      throws Exception {
    CanonicalGroupArithmeticEmptyStateTest.assertEmpty(
        mode, rocks, ttl, true, key.equals("int"), key.equals("global"));
  }
}
