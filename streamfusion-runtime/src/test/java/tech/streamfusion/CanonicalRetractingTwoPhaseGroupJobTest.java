package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalRetractingTwoPhaseGroupJobTest extends CanonicalGroupArithmeticJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("same-cut-stock-reference"));
    Files.writeString(root.resolve("two-phase-mini-batch"), "5");
    Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-global-group-aggregate");
    return root;
  }
}
