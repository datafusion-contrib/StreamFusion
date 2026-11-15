package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryWindowJoinJobTest extends CanonicalWindowJoinJobTest {
  @Override
  void rescaleInput(Path root) throws Exception {
    super.rescaleInput(root);
    Files.createFile(root.resolve("disable-chaining"));
  }

  @Override
  Path prepare(String name, String kind, String shape, boolean keyless) throws Exception {
    Path root = super.prepare(name, kind, shape, keyless);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
