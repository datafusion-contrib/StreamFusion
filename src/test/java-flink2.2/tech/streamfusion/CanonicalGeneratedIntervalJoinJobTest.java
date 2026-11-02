package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalGeneratedIntervalJoinJobTest extends CanonicalIntervalJoinJobTest {
  @Override
  Path prepare(String name, String kind, boolean processingTime, boolean residual)
      throws Exception {
    Path root = super.prepare(name, kind, processingTime, residual);
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }
}
