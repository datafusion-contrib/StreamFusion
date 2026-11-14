package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalVariableExtremaMiniBatchJobTest extends CanonicalVariableExtremaKeyJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("one-phase-mini-batch"), "7");
    return root;
  }
}
