package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalCompositeExtremaMiniBatchJobTest extends CanonicalCompositeExtremaKeyJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("one-phase-mini-batch"), "7");
    Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    Files.createFile(root.resolve("same-cut-stock-reference"));
    return root;
  }
}
