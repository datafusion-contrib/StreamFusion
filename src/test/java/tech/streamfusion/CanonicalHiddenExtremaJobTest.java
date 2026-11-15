package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalHiddenExtremaJobTest extends CanonicalRetractingExtremaJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(arguments -> arguments.get()[0].equals("continue"));
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,MIN(v)+SUM(v),MAX(v)+AVG(v) FROM input_table GROUP BY"
            + " k");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    return Map.of("null", "null|null", "2", "22|22", "3", "16|11");
  }
}
