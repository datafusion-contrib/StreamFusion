package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalMiniBatchManagedUdfGroupJobTest extends CanonicalManagedUdfGroupJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("one-phase-mini-batch"), "5");
    Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    Files.writeString(root.resolve("require-checkpoint-flush-families"), "NativeColumnarJvmGroup");
    Files.createFile(root.resolve("same-cut-stock-reference"));
    return root;
  }
}
