package tech.streamfusion;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;

/** Module-owned resource accounting used by the shared test lifecycle. */
public interface TestResourceAudit {
  void initialize(Configuration configuration);

  void assertReleased(String scenario) throws InterruptedException;

  static List<TestResourceAudit> load(ClassLoader loader) {
    var audits =
        ServiceLoader.load(TestResourceAudit.class, loader).stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toList());
    if (audits.isEmpty()) {
      throw new IllegalStateException("Shared tests require a module-owned resource audit");
    }
    return audits;
  }
}
