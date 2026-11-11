package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGroupArithmeticIntJobTest extends CanonicalGroupArithmeticAppendJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases().filter(test -> test.get()[0].equals("rescale"));
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    var input = Files.readAllLines(root.resolve("input.csv"));
    input.replaceAll(
        line -> {
          var cells = line.split(",", -1);
          if (!cells[1].equals("null")) cells[1] = Long.toString(-Long.parseLong(cells[1]));
          return String.join(",", cells);
        });
    Files.write(root.resolve("input.csv"), input);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(CAST(k AS INT) AS BIGINT),SUM(v),AVG(v) FROM"
            + " input_table GROUP BY CAST(k AS INT)");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var result = new java.util.HashMap<String, String>();
    super.expectedTable(mode)
        .forEach(
            (key, value) ->
                result.put(key.equals("null") ? key : Long.toString(-Long.parseLong(key)), value));
    return result;
  }
}
