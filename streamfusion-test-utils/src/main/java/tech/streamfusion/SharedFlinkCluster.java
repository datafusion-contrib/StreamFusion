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

/**
 * Starts one MiniCluster on demand per launcher session, with fresh execution contexts and native
 * leak checks for every test. Launcher-session ownership closes the cluster when the test run ends.
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
    initializeResources(config);
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
    if (lazyStart()) initializeResources(configuration().getConfiguration());
    else start(cluster);
  }

  private static boolean lazyStart() {
    return Boolean.parseBoolean(System.getProperty("sf.test.lazyCluster", "true"));
  }

  private static void start(SessionCluster cluster) throws Exception {
    cluster.start();
    initializeResources(cluster.getMiniCluster().getConfiguration());
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    var cluster = session(context);
    if (lazyStart()) {
      FlinkTestContexts.deferForLocalTests(
          () -> {
            try {
              start(cluster);
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot start the shared test cluster", failure);
            }
            FlinkTestContexts.setForLocalTests(cluster.getMiniCluster(), cluster.getNumberSlots());
          });
    } else FlinkTestContexts.setForLocalTests(cluster.getMiniCluster(), cluster.getNumberSlots());
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    FlinkTestContexts.clearForRemoteSubmission();
    session(context).cancelRemainingJobs();
    assertNativeMemoryReleased(context.getDisplayName());
  }

  static void assertNativeMemoryReleased(String scenario) throws InterruptedException {
    for (var audit : Resources.AUDITS) audit.assertReleased(scenario);
  }

  private static void initializeResources(Configuration configuration) {
    for (var audit : Resources.AUDITS) audit.initialize(configuration);
  }

  private static final class Resources {
    private static final java.util.List<TestResourceAudit> AUDITS =
        TestResourceAudit.load(SharedFlinkCluster.class.getClassLoader());
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
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (true) {
        var resources = cluster.getResourceOverview().get(10, TimeUnit.SECONDS);
        if (resources.getNumberFreeSlots() == getNumberSlots()) return;
        if (System.nanoTime() > deadline)
          throw new AssertionError(
              "Shared test cluster did not release its slots: "
                  + resources.getNumberFreeSlots()
                  + " free of "
                  + getNumberSlots());
        Thread.sleep(20);
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
