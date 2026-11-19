package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryUpdateTopNJobTest extends CanonicalUpdateTopNJobTest {
  @Override
  Path prepare(String name, String variant, String mode) throws Exception {
    Path root = super.prepare(name, variant, mode);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
