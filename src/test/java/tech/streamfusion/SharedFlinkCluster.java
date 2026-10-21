package tech.streamfusion;

import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import tech.streamfusion.compat.FlinkTestContexts;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/**
 * Reuses one MiniCluster per launcher session (one Surefire fork), with fresh execution contexts
 * and native leak checks for every test. Launcher-session ownership closes the cluster when the
 * test run ends.
 */
public final class SharedFlinkCluster
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(SharedFlinkCluster.class);

  private static MiniClusterResourceConfiguration configuration() {
    // Task off-heap sized like a tuned deployment: the TaskManager-wide StreamFusion pool accounts
    // native state, Arrow buffers, and connector queues against this normal Flink process budget.
    // Sized for the benchmark
    // suite's multi-million-event runs (the tuned mini-batch column runs 5M events; q4's regular
    // join keeps BOTH sides' full 5M-event state, and its share of one slot must hold all of it).
    // Reserving task off-heap is
    // bookkeeping, not allocation — the larger size costs nothing when unused. Note the extension
    // redirects getExecutionEnvironment() here, so per-test cluster Configurations are ignored; a
    // test that needs a different memory setup must build its own local environment
    // (FlinkMemoryAccountingTest does).
    Configuration config = new Configuration();
    String managedMemory = System.getenv("SF_BENCH_MANAGED_MEMORY");
    if (managedMemory != null) {
      config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse(managedMemory));
    }
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("48g"));
    // Direct operator test harnesses expose an empty mock TaskManager configuration. Initialize
    // the same process-wide authority here before either a harness or the shared MiniCluster opens.
    TaskOffHeapMemory.initialize(config);
    // Fail fast, never loop: checkpointing enables Flink's default infinite fixed-delay restarts,
    // under which a deterministically failing job restarts forever while collect() polls — a
    // silent multi-hour hang where a test failure should be. A real deployment tunes its own
    // restart strategy; a test's job either works or should fail its assertion promptly.
    config.setString("restart-strategy.type", "none");
    return new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(config)
        .setNumberTaskManagers(1)
        .setNumberSlotsPerTaskManager(
            managedMemory == null
                ? 8
                : Integer.parseInt(System.getenv().getOrDefault("SF_PARALLELISM", "4")))
        .build();
  }

  static SessionCluster session(ExtensionContext context) {
    var sessionCluster = SharedFlinkClusterSession.currentCluster();
    if (sessionCluster != null) return sessionCluster;
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            SessionCluster.class, key -> new SessionCluster(), SessionCluster.class);
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    var cluster = session(context);
    cluster.start();
    TaskOffHeapMemory.initialize(cluster.getMiniCluster().getConfiguration());
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    var cluster = session(context);
    FlinkTestContexts.setForLocalTests(cluster.getMiniCluster(), cluster.getNumberSlots());
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    FlinkTestContexts.clearForRemoteSubmission();
    assertNativeMemoryReleased(context.getDisplayName());
  }

  /**
   * Asserts the native side is quiescent after every test, turning the whole suite into a standing
   * leak audit: every native handle created over JNI must have been closed (a live handle is Rust
   * memory the JVM's tooling cannot see), and the shared Arrow FFI allocator must hold zero bytes
   * (a nonzero balance is an unclosed {@code VectorSchemaRoot} or a dropped C Data release
   * callback, in either transfer direction — imports are registered as foreign allocations). The
   * task pool must also release every reservation, including native state outside Arrow.
   *
   * <p>Task cleanup can trail the job result by a moment (source fetcher threads close
   * asynchronously), so the check polls briefly before failing; when clean it costs one JNI call
   * per loaded library.
   */
  static void assertNativeMemoryReleased(String scenario) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    String handles = NativeExtensionLoader.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long reserved = TaskOffHeapMemory.reservedBytes();
    while ((!handles.isEmpty() || allocated != 0 || reserved != 0)
        && System.nanoTime() < deadline) {
      // Records a failed job dropped in flight are freed by the ArrowBatch cleaner backstop, which
      // only runs once a GC notices they are unreachable — nudge it rather than waiting one out.
      System.gc();
      Thread.sleep(20);
      handles = NativeExtensionLoader.liveNativeHandles();
      allocated = NativeAllocator.SHARED.getAllocatedMemory();
      reserved = TaskOffHeapMemory.reservedBytes();
    }
    if (!handles.isEmpty() || allocated != 0 || reserved != 0) {
      throw new AssertionError(
          "native memory outstanding after "
              + scenario
              + ": live native handles ["
              + handles
              + "], Arrow allocator holds "
              + allocated
              + " bytes, task pool reserves "
              + reserved
              + " bytes\n"
              + NativeAllocator.SHARED.toVerboseString());
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    session(context).cancelRemainingJobs();
  }

  static final class SessionCluster extends MiniClusterResource
      implements ExtensionContext.Store.CloseableResource {
    private boolean started;

    SessionCluster() {
      super(configuration());
    }

    synchronized void start() throws Exception {
      if (!started) {
        try {
          before();
          started = true;
          System.out.println(
              "StreamFusion test MiniCluster started pid=" + ProcessHandle.current().pid());
        } catch (Exception failure) {
          after();
          throw failure;
        }
      }
    }

    void cancelRemainingJobs() throws Exception {
      if (!started) return;
      var cluster = getMiniCluster();
      for (var job : cluster.listJobs().get(30, TimeUnit.SECONDS)) {
        if (job.getJobState().isGloballyTerminalState()) continue;
        try {
          cluster.cancelJob(job.getJobId()).get(30, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
          if (!cluster
              .getJobStatus(job.getJobId())
              .get(30, TimeUnit.SECONDS)
              .isGloballyTerminalState()) throw failure;
        }
        cluster.requestJobResult(job.getJobId()).get(30, TimeUnit.SECONDS);
      }
    }

    @Override
    public void close() {
      if (started) {
        after();
        started = false;
      }
    }
  }
}
