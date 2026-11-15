package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryFirstNJobTest extends CanonicalFirstNJobTest {
  @Override
  Path prepare(String name, String mode) throws Exception {
    Path root = super.prepare(name, mode);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
