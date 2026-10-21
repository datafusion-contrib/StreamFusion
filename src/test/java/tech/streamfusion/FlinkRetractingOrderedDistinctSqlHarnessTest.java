package tech.streamfusion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRetractingOrderedDistinctSqlHarnessTest {
  static Stream<Arguments> profiles() {
    return Stream.of("FLOAT", "DOUBLE", "DECIMAL(38,0)", "DECIMAL(38,37)")
        .flatMap(type -> Stream.of(1, 3, 7, 32).map(bundle -> Arguments.of(type, bundle)));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void removalsReinsertionsAndSharedFiltersPreserveArithmeticOrder(String type, int bundle) {
    var rows = new ArrayList<Row>();
    rows.add(Row.of(0, null, false));
    var values = new ArrayList<String>();
    if (type.equals("FLOAT"))
      values.addAll(List.of("16777216", "1", "-16777216", "3", "-0.0", "0.0"));
    if (type.equals("DOUBLE"))
      values.addAll(List.of("9007199254740992", "1", "-9007199254740992", "3", "-0.0", "0.0"));
    for (int i = 0; i < 96; i++) {
      String value;
      if (type.equals("FLOAT"))
        value = Float.toString(Float.intBitsToFloat(0x3f000000 + i * 65537));
      else if (type.equals("DOUBLE"))
        value =
            Double.toString(Double.longBitsToDouble(0x3ff0000000000000L | ((long) i << 32) | i));
      else {
        BigInteger unscaled =
            BigInteger.TEN.pow(37).multiply(BigInteger.valueOf(9)).add(BigInteger.valueOf(i));
        if (i % 2 != 0) unscaled = unscaled.negate();
        value = new BigDecimal(unscaled, type.endsWith(",37)") ? 37 : 0).toPlainString();
      }
      values.add(value);
    }
    for (int i = 0; i < values.size(); i++) rows.add(row(RowKind.INSERT, values.get(i), keep(i)));
    for (int i = 0; i < 20; i++)
      rows.add(row(RowKind.INSERT, values.get(i), !Boolean.TRUE.equals(keep(i))));
    for (int i = 0; i < 60; i++) rows.add(row(RowKind.DELETE, values.get(i), keep(i)));
    for (int i = 60; i < values.size(); i++) {
      rows.add(row(RowKind.DELETE, values.get(i), keep(i)));
      rows.add(row(RowKind.INSERT, values.get(i), !Boolean.TRUE.equals(keep(i))));
    }
    for (int i = 0; i < 50; i++) rows.add(row(RowKind.INSERT, values.get(i), keep(i)));
    FlinkRetractingDistinctPartialsSqlHarnessTest.compare(
        type,
        bundle,
        true,
        rows,
        type.equals("DECIMAL(38,37)")
            && bundle <= 3
            && org.apache.flink.runtime.util.EnvironmentInformation.getVersion()
                .startsWith("1.18."));
  }

  private static Boolean keep(int i) {
    return i % 3 == 0 ? null : i % 3 == 1;
  }

  private static Row row(RowKind kind, String value, Boolean keep) {
    return Row.ofKind(kind, 0, value, keep);
  }
}
