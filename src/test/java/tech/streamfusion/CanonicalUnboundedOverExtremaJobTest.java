package tech.streamfusion;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.compat.FlinkPlanCompat;

class CanonicalUnboundedOverExtremaJobTest extends CanonicalUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
  }

  private static Stream<Arguments> cases(boolean recovery) {
    return CanonicalTestMatrix.select(
        Stream.of("proc", "event")
            .flatMap(
                clock ->
                    Stream.of("rows", "range")
                        .flatMap(
                            frame ->
                                Stream.of(
                                        "BIGINT",
                                        "TINYINT",
                                        "DECIMAL(18,2)",
                                        "DECIMAL(38,20)",
                                        "STRING")
                                    .flatMap(
                                        type ->
                                            Stream.of(1, FlinkPlanCompat.unboundedOverVersion())
                                                .distinct()
                                                .filter(
                                                    version ->
                                                        clock.equals("event") || version == 1)
                                                .flatMap(
                                                    version ->
                                                        CanonicalGroupArithmeticJobTest.cases()
                                                            .filter(
                                                                args ->
                                                                    recovery
                                                                        != args.get()[0].equals(
                                                                            "continue"))
                                                            .map(
                                                                args ->
                                                                    Arguments.of(
                                                                        clock + ":" + frame + ":"
                                                                            + type + ":" + version,
                                                                        args.get()[0],
                                                                        args.get()[1],
                                                                        args.get()[2],
                                                                        args.get()[3])))))));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var parts = kind.split(":");
    boolean event = parts[0].equals("event");
    var root = super.prepare(name, parts[1], mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v "
            + parts[2]
            + ",rt TIMESTAMP(3)"
            + (event ? ",WATERMARK FOR rt AS SOURCE_WATERMARK()" : ""));
    Files.writeString(root.resolve("unbounded-over-version"), parts[3]);
    if (event) {
      Files.createFile(root.resolve("source-watermarks"));
      var input = new ArrayList<String>();
      var original = Files.readAllLines(root.resolve("input.csv"));
      for (int index = 0; index < original.size(); index++) {
        input.add(original.get(index));
        if (!mode.equals("rescale") && (index == 3 || index == 6))
          input.add("WATERMARK," + (index == 3 ? "1000" : "1003"));
      }
      input.add("WATERMARK,1100");
      Files.write(root.resolve("input.csv"), input);
      Files.writeString(root.resolve("state-ttl"), "1 h");
    }
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,CAST(MIN(v) OVER w AS BIGINT),"
            + "CAST(MAX(v) OVER w AS BIGINT) FROM input_table WINDOW w AS (PARTITION BY k ORDER BY "
            + (event ? "rt" : "pt")
            + (parts[1].equals("rows") ? " ROWS" : " RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (mode.equals("rescale")) {
      var result = new HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        result.put(key == 0 ? "null" : Integer.toString(key), "5|6");
      return result;
    }
    return kind.contains(":STRING:")
        ? Map.of("1", "17|9", "2", "11|2", "null", "3|5")
        : Map.of("1", "4|17", "2", "2|11", "null", "3|5");
  }
}
