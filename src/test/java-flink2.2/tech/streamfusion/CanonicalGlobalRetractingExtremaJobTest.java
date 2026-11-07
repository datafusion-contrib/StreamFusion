package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

class CanonicalGlobalRetractingExtremaJobTest extends CanonicalGlobalGroupArithmeticJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(0 AS BIGINT),MIN(v),MAX(v) FROM input_table");
    return root;
  }

  @Override
  void configureBackend(Path root, String backend) throws Exception {
    CanonicalRetractingExtremaJobTest.configureRawStorage(root, backend);
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    return Map.of("0", mode.equals("rescale") ? "5|6" : "5|11");
  }
}
