package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalGeneratedTemporalJoinJobTest extends CanonicalTemporalJoinJobTest {
  @Override
  void rescaleInput(Path root) throws Exception {
    super.rescaleInput(root);
    Files.createFile(root.resolve("disable-chaining"));
  }

  @Override
  Path prepare(String name, String kind, boolean residual) throws Exception {
    Path root = super.prepare(name, kind, residual);
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }
}
