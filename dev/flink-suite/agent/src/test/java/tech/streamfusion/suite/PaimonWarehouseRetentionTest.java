package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PaimonWarehouseRetentionTest {
  @TempDir Path directory;

  private static class Fixture {
    private final String path;

    Fixture(Path path) {
      this.path = path.toString();
    }
  }

  @Test
  void retainsFailedWarehouseBeforeFixtureCleanup() throws Exception {
    verifyRetention(true, true, true);
  }

  @Test
  void leavesWarehouseUncopiedWhenRetentionIsDisabled() throws Exception {
    verifyRetention(false, true, true);
  }

  @Test
  void doesNotCopySuccessfulFixtures() throws Exception {
    verifyRetention(true, false, true);
  }

  @Test
  void reportsCaptureErrorsWithoutReplacingTheTestFailure() throws Exception {
    verifyRetention(true, true, false);
  }

  private void verifyRetention(boolean enabled, boolean failed, boolean sourceExists)
      throws Exception {
    String option = "streamfusion.flink-suite.retain-failed-warehouse";
    String outputOption = "streamfusion.flink-suite.diagnostics";
    String previous = System.getProperty(option);
    String previousOutput = System.getProperty(outputOption);
    var warehouse = directory.resolve("warehouse");
    var snapshot = warehouse.resolve("default.db/T/snapshot/snapshot-1");
    var manifest = warehouse.resolve("default.db/T/manifest/manifest-1");
    var data = warehouse.resolve("default.db/T/pt=0/bucket-0/data.parquet");
    Files.createDirectories(snapshot.getParent());
    Files.createDirectories(manifest.getParent());
    Files.createDirectories(data.getParent());
    Files.writeString(snapshot, "snapshot metadata");
    Files.writeString(manifest, "manifest metadata");
    Files.write(data, new byte[] {0, 1, 2, 3});
    var output = directory.resolve("diagnostics");
    try {
      System.setProperty(option, Boolean.toString(enabled));
      System.setProperty(outputOption, output.toString());
      var diagnostics = new ByteArrayOutputStream();
      PaimonTestWatch.initialize(new PrintStream(diagnostics));
      var watch =
          PaimonTestWatch.start(
              "fixture.test",
              new Fixture(sourceExists ? warehouse : directory.resolve("missing")));
      assertDoesNotThrow(() -> watch.finish(failed ? new AssertionError("wrong final value") : null));
      if (enabled && failed && sourceExists) {
        try (var captures = Files.list(output.resolve("failed-warehouses"))) {
          var entries = captures.toList();
          assertEquals(1, entries.size());
          assertEquals(
              "snapshot metadata",
              Files.readString(entries.get(0).resolve(warehouse.relativize(snapshot))));
          assertEquals(
              "manifest metadata",
              Files.readString(entries.get(0).resolve(warehouse.relativize(manifest))));
          org.junit.jupiter.api.Assertions.assertArrayEquals(
              new byte[] {0, 1, 2, 3},
              Files.readAllBytes(entries.get(0).resolve(warehouse.relativize(data))));
        }
      } else if (!enabled || !failed) {
        assertFalse(Files.exists(output));
      } else {
        assertTrue(diagnostics.toString().contains("wrong final value"));
        assertTrue(diagnostics.toString().contains("Failed to retain Paimon warehouse:"));
      }
    } finally {
      restore(option, previous);
      restore(outputOption, previousOutput);
      PaimonTestWatch.initialize(System.err);
    }
  }

  private static void restore(String option, String value) {
    if (value == null) {
      System.clearProperty(option);
    } else {
      System.setProperty(option, value);
    }
  }
}
