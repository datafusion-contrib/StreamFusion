package tech.streamfusion.planner.stream;

import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGlobalGroupArithmeticInitializationTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticInitializationTest.cases()
        .flatMap(
            test ->
                Stream.of(false, true)
                    .map(
                        append ->
                            Arguments.of(
                                test.get()[0],
                                test.get()[1],
                                test.get()[2],
                                test.get()[3],
                                test.get()[4],
                                append)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void globalGeneratedStateRetainsTheEmptyStockKey(
      String mode, boolean rocks, long ttl, boolean first, boolean before, boolean append)
      throws Exception {
    var steps =
        List.of(
            input(mode, null, 0),
            input(mode, 11L, 0),
            input(mode, 5L, 0),
            input(mode, 7L, 0),
            input(mode, 4L, 0),
            input(mode, append ? -11L : 11L, append ? 0 : 3),
            input(mode, append ? -7L : 7L, append ? 0 : 1),
            input(mode, append ? -4L : 4L, append ? 0 : 3),
            input(mode, append ? -5L : 5L, append ? 0 : 3),
            input(mode, null, append ? 0 : 3),
            input(mode, 17L, 0),
            input(mode, null, 0));
    assertLifecycle(mode, rocks, ttl, first, before, append, false, true, steps);
  }

  static Stream<Arguments> emptyCases() {
    return CanonicalGroupArithmeticEmptyStateTest.cases()
        .filter(test -> !(boolean) test.get()[4])
        .map(test -> Arguments.of(test.get()[0], test.get()[1], test.get()[2], test.get()[3]));
  }

  @ParameterizedTest
  @MethodSource("emptyCases")
  void initialEmptyGlobalStateDoesNotInventASqlResult(
      String mode, boolean rocks, long ttl, boolean append) throws Exception {
    CanonicalGroupArithmeticEmptyStateTest.assertEmpty(mode, rocks, ttl, append, false, true);
  }

  static Stream<Arguments> recoveryCases() {
    return CanonicalGroupArithmeticRecoveryTest.cases()
        .flatMap(
            test ->
                Stream.of(false, true)
                    .map(
                        append ->
                            Arguments.of(test.get()[0], test.get()[1], test.get()[2], append)));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void nativeGlobalCheckpointsReturnToStockCanonicalState(
      String mode, boolean rocks, long ttl, boolean append) throws Exception {
    CanonicalGroupArithmeticRecoveryTest.assertNativeRecovery(mode, rocks, ttl, append, true);
  }

  private static GenericRowData input(String mode, Long value, int kind) {
    var row = mode.equals("star") ? GenericRowData.of(null, value) : GenericRowData.of(value);
    row.setRowKind(org.apache.flink.types.RowKind.fromByteValue((byte) kind));
    return row;
  }
}
