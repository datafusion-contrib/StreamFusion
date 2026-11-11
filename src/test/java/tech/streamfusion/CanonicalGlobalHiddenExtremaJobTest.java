package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGlobalHiddenExtremaJobTest extends CanonicalGlobalRetractingExtremaJobTest {
  static Stream<Arguments> cases() {
    return CanonicalHiddenExtremaJobTest.cases();
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(0 AS BIGINT),MIN(v)+SUM(v),MAX(v)+AVG(v) FROM"
            + " input_table");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    return Map.of("0", "27|18");
  }
}
