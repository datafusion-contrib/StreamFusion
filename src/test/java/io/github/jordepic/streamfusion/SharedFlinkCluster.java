package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.operator.NativeAllocator;
import java.util.concurrent.TimeUnit;
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
 *
 * <p>Being registered around every test also makes this the suite-wide native leak check: see
 * {@link #assertNativeMemoryReleased}.
 */
public final class SharedFlinkCluster
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private final MiniClusterExtension cluster;

  public SharedFlinkCluster() {
    // Managed memory sized like a tuned deployment rather than the test-utils 80m default: the
    // native stateful operators reserve their slot's managed share and enforce it as a hard state
    // budget, so under the default the Nexmark changelog queries (updating joins, Top-N) fail on
    // state a real TaskManager (40% of process memory) holds easily. Sized for the benchmark
    // suite's multi-million-event runs (the tuned mini-batch column runs 5M events; q4's regular
    // join keeps BOTH sides' full 5M-event state, and its share of one slot must hold all of it).
    // Reserving managed memory is
    // bookkeeping, not allocation — the larger size costs nothing when unused. Note the extension
    // redirects getExecutionEnvironment() here, so per-test cluster Configurations are ignored; a
    // test that needs a different memory setup must build its own local environment
    // (FlinkMemoryAccountingTest does).
    Configuration config = new Configuration();
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48g"));
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
    assertNativeMemoryReleased(context);
  }

  /**
   * Asserts the native side is quiescent after every test, turning the whole suite into a standing
   * leak audit: every native handle created over JNI must have been closed (a live handle is Rust
   * memory the JVM's tooling cannot see), and the shared Arrow FFI allocator must hold zero bytes
   * (a nonzero balance is an unclosed {@code VectorSchemaRoot} or a dropped C Data release
   * callback, in either transfer direction — imports are registered as foreign allocations).
   *
   * <p>Task cleanup can trail the job result by a moment (source fetcher threads close
   * asynchronously), so the check polls briefly before failing; when clean it costs one JNI call.
   */
  private static void assertNativeMemoryReleased(ExtensionContext context)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    while ((!handles.isEmpty() || allocated != 0) && System.nanoTime() < deadline) {
      // Records a failed job dropped in flight are freed by the ArrowBatch cleaner backstop, which
      // only runs once a GC notices they are unreachable — nudge it rather than waiting one out.
      System.gc();
      Thread.sleep(20);
      handles = Native.liveNativeHandles();
      allocated = NativeAllocator.SHARED.getAllocatedMemory();
    }
    if (!handles.isEmpty() || allocated != 0) {
      throw new AssertionError(
          "native memory outstanding after "
              + context.getDisplayName()
              + ": live native handles ["
              + handles
              + "], Arrow allocator holds "
              + allocated
              + " bytes\n"
              + NativeAllocator.SHARED.toVerboseString());
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    cluster.afterAll(context);
  }
}
