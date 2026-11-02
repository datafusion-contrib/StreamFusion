package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryTemporalJoinJobTest extends CanonicalTemporalJoinJobTest {
  @Override
  void rescaleInput(Path root) throws Exception {
    super.rescaleInput(root);
    Files.createFile(root.resolve("disable-chaining"));
  }

  @Override
  Path prepare(String name, String kind, boolean residual) throws Exception {
    Path root = super.prepare(name, kind, residual);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
