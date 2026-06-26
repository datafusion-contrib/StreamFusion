package io.github.jordepic.streamfusion;

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

  private final MiniClusterExtension cluster =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(8)
              .build());

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
