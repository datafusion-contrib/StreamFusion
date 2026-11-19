package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUnboundedOverJobTest extends CanonicalBoundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return unboundedCases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return unboundedCases(true);
  }

  private static Stream<Arguments> unboundedCases(boolean recovery) {
    return Stream.of("rows", "range")
        .flatMap(
            kind ->
                CanonicalGroupArithmeticJobTest.cases()
                    .filter(args -> recovery != args.get()[0].equals("continue"))
                    .map(
                        args ->
                            Arguments.of(
                                kind, args.get()[0], args.get()[1], args.get()[2], args.get()[3])));
  }

  boolean global() {
    return false;
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, "processing-rows", mode, ordinary);
    if (global()) Files.createFile(root.resolve("global-over"));
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT "
            + (global() ? "CAST(0 AS BIGINT)" : "k")
            + ",SUM(v) OVER w,AVG(v) OVER w FROM input_table WINDOW w AS ("
            + (global() ? "" : "PARTITION BY k ")
            + "ORDER BY pt "
            + (kind.equals("rows") ? "ROWS" : "RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (global()) return Map.of("0", mode.equals("rescale") ? "2112|5" : "58|7");
    if (!mode.equals("rescale")) return Map.of("1", "37|9", "2", "13|6", "null", "8|4");
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), "33|5");
    return expected;
  }
}
