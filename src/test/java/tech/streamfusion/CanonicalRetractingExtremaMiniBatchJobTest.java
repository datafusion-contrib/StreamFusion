package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalRetractingExtremaMiniBatchJobTest extends CanonicalGroupArithmeticJobTest {
  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(CanonicalGroupArithmeticJobTest.cases());
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("same-cut-stock-reference"));
    Files.writeString(root.resolve("one-phase-mini-batch"), "5");
    Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,MIN(v),MAX(v) FROM input_table GROUP BY k");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    if (mode.equals("rescale")) {
      var expected = new java.util.HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), "5|6");
      return expected;
    }
    return Map.of("null", "null|null", "2", "11|11", "3", "5|6");
  }
}
