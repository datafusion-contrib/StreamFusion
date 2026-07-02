package io.github.jordepic.streamfusion;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Auto-registered (see {@code src/test/resources/junit-platform.properties} and the JUnit extension
 * service file) for every test class, this shares one Flink mini-cluster per test class instead of the
 * default of booting and tearing down a fresh local cluster on each {@code executeSql().collect()} /
 * {@code env.execute()}. Flink's {@link MiniClusterExtension} starts the cluster once (per class) and
 * redirects {@code StreamExecutionEnvironment.getExecutionEnvironment()} to it, so the SQL harness tests
 * submit their jobs to the shared cluster — removing the suite's dominant per-test cost.
 *
 * <p>{@code MiniClusterExtension} is {@code final}, so this composes it rather than subclassing, only to
 * supply a slot count large enough for the tests that run at parallelism &gt; 1. Tests that never touch a
 * cluster (the operator harness unit tests) simply ignore it.
 */
public final class SharedFlinkCluster
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private final MiniClusterExtension cluster;

  public SharedFlinkCluster() {
    // Managed memory sized like a tuned deployment rather than the test-utils 80m default: the
    // native stateful operators reserve their slot's managed share and enforce it as a hard state
    // budget, so under the default the Nexmark changelog queries (updating joins, Top-N) fail on
    // state a real TaskManager (40% of process memory) holds easily. Reserving managed memory is
    // bookkeeping, not allocation — the larger size costs nothing when unused. Note the extension
    // redirects getExecutionEnvironment() here, so per-test cluster Configurations are ignored; a
    // test that needs a different memory setup must build its own local environment
    // (FlinkMemoryAccountingTest does).
    Configuration config = new Configuration();
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("6g"));
    cluster =
        new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(config)
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(8)
                .build());
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    cluster.beforeAll(context);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    cluster.beforeEach(context);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    cluster.afterEach(context);
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    cluster.afterAll(context);
  }
}
