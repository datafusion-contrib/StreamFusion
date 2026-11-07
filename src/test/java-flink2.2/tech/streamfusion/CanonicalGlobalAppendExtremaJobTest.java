package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

class CanonicalGlobalAppendExtremaJobTest extends CanonicalGlobalGroupArithmeticJobTest {
  @Override
  boolean appendOnly() {
    return true;
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(0 AS BIGINT),MIN(v),MAX(v) FROM input_table");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    return Map.of("0", mode.equals("rescale") ? "5|6" : "-20|20");
  }
}
