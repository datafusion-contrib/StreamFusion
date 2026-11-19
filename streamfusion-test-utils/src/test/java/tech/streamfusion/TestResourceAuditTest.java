package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestResourceAuditTest {
  @TempDir Path directory;

  @Test
  void missingAuditFailsInsteadOfDisablingLeakChecks() throws Exception {
    ClassLoader loader = loader("");
    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> TestResourceAudit.load(loader));
    assertTrue(error.getMessage().contains("module-owned resource audit"));
  }

  @Test
  void discoveredAuditPreservesConfigurationAndFailure() throws Exception {
    var audits = TestResourceAudit.load(loader(RecordingAudit.class.getName()));
    assertEquals(1, audits.size());
    RecordingAudit audit = (RecordingAudit) audits.get(0);
    Configuration configuration = new Configuration();
    audit.initialize(configuration);
    assertSame(configuration, audit.configuration);
    AssertionError error =
        assertThrows(AssertionError.class, () -> audit.assertReleased("failed job cleanup"));
    assertEquals("failed job cleanup", error.getMessage());
  }

  private ClassLoader loader(String providers) throws IOException {
    Path registration = directory.resolve("providers");
    Files.writeString(registration, providers);
    URL resource = registration.toUri().toURL();
    return new ClassLoader(TestResourceAudit.class.getClassLoader()) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        return name.equals("META-INF/services/" + TestResourceAudit.class.getName())
            ? Collections.enumeration(Collections.singletonList(resource))
            : super.getResources(name);
      }
    };
  }

  public static final class RecordingAudit implements TestResourceAudit {
    Configuration configuration;

    @Override
    public void initialize(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public void assertReleased(String scenario) {
      throw new AssertionError(scenario);
    }
  }
}
