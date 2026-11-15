package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalDecimalMiniBatchGroupJobTest extends CanonicalDecimalGroupJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("same-cut-stock-reference"));
    Files.writeString(root.resolve("one-phase-mini-batch"), "5");
    Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    return root;
  }
}
