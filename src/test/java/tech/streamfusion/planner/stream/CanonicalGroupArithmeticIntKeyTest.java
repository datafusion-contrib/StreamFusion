package tech.streamfusion.planner.stream;

import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupArithmeticIntKeyTest {
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
  void intKeysKeepTheirStockBinaryEncodingAndSerializer(
      String mode, boolean rocks, long ttl, boolean first, boolean before, boolean append)
      throws Exception {
    var steps =
        List.of(
            intRow(Integer.MIN_VALUE, null, 0),
            intRow(Integer.MAX_VALUE, 11L, 0),
            intRow(null, 5L, 0),
            intRow(Integer.MIN_VALUE, 7L, 0),
            intRow(Integer.MIN_VALUE, 4L, 0),
            intRow(Integer.MAX_VALUE, append ? -11L : 11L, append ? 0 : 3),
            intRow(Integer.MIN_VALUE, append ? -7L : 7L, append ? 0 : 1),
            intRow(Integer.MIN_VALUE, append ? -4L : 4L, append ? 0 : 3),
            intRow(null, append ? -5L : 5L, append ? 0 : 3),
            intRow(Integer.MIN_VALUE, null, append ? 0 : 3),
            intRow(Integer.MIN_VALUE, 17L, 0),
            intRow(Integer.MAX_VALUE, null, 0));
    assertLifecycle(mode, rocks, ttl, first, before, append, true, steps);
  }

  private static GenericRowData intRow(Integer key, Long value, int kind) {
    var row = GenericRowData.of(key, value);
    row.setRowKind(RowKind.fromByteValue((byte) kind));
    return row;
  }
}
