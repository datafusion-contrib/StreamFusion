package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryRetractingAggregateGraphJobTest
    extends CanonicalRetractingAggregateGraphJobTest {
  @Override
  Path prepare(String name, String variant, String mode) throws Exception {
    var root = super.prepare(name, variant, mode);
    Files.createFile(root.resolve("ordinary-sql"));
    return root;
  }
}
