package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryFirstNRowKeyJobTest extends CanonicalFirstNRowKeyJobTest {
  @Override
  Path prepare(String name, String mode) throws Exception {
    var root = super.prepare(name, mode);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
