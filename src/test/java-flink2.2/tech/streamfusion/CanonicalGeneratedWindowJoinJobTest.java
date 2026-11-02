package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalGeneratedWindowJoinJobTest extends CanonicalWindowJoinJobTest {
  @Override
  Path prepare(String name, String kind, String shape, boolean keyless) throws Exception {
    Path root = super.prepare(name, kind, shape, keyless);
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }
}
