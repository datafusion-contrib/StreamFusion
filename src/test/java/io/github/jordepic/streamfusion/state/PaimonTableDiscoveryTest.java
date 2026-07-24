package io.github.jordepic.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The compaction path discovers every Paimon table under an operator's state directory: a single
 * table rooted at the directory itself, or one table per immediate child (a multi-state operator
 * like the join) — presence of a {@code schema/} dir being the ground truth either way.
 */
class PaimonTableDiscoveryTest {

  @Test
  void singleTableIsTheDirectoryItself() throws Exception {
    File root = Files.createTempDirectory("paimon-discovery-single").toFile();
    new File(root, "schema").mkdirs();
    new File(root, "bucket-0").mkdirs();
    assertEquals(List.of(root), PaimonSnapshotStrategy.discoverTables(root));
  }

  @Test
  void multiTableLayoutListsEachChildWithASchema() throws Exception {
    File root = Files.createTempDirectory("paimon-discovery-multi").toFile();
    File left = new File(root, "left");
    File right = new File(root, "right");
    new File(left, "schema").mkdirs();
    new File(right, "schema").mkdirs();
    new File(root, "checkpoints").mkdirs(); // not a table — no schema dir
    assertEquals(List.of(left, right), PaimonSnapshotStrategy.discoverTables(root));
  }

  @Test
  void emptyDirectoryHasNoTables() throws Exception {
    File root = Files.createTempDirectory("paimon-discovery-empty").toFile();
    assertEquals(List.of(), PaimonSnapshotStrategy.discoverTables(root));
  }
}
