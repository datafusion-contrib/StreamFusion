package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryRegularJoinJobTest extends CanonicalRegularJoinJobTest {
  @Override
  Path prepare(String name, String kind, String shape) throws Exception {
    Path root = super.prepare(name, kind, shape);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
