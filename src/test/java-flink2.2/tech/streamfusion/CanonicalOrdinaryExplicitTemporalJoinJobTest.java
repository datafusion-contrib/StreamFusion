package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalOrdinaryExplicitTemporalJoinJobTest extends CanonicalOrdinaryTemporalJoinJobTest {
  @Override
  Path prepare(String name, String kind, boolean residual) throws Exception {
    Path root = super.prepare(name, kind, residual);
    Files.writeString(root.resolve("uid-generation"), "ALWAYS");
    return root;
  }
}
