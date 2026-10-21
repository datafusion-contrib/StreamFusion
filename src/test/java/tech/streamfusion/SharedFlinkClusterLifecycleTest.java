package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import tech.streamfusion.compat.StreamTestSinks;
import tech.streamfusion.operator.TaskOffHeapMemory;

class SharedFlinkClusterLifecycleTest {
  private static final List<MiniCluster> clusters = new ArrayList<>();
  private static JobClient unfinished;

  @Test
  void reusesClusterAcrossRootsAndFailuresThenClosesSession() throws Exception {
    clusters.clear();
    unfinished = null;
    var first = new SummaryGeneratingListener();
    var second = new SummaryGeneratingListener();
    try (var launcher = LauncherFactory.openSession()) {
      for (var fixture : List.of(FirstFixture.class, SecondFixture.class)) {
        var request =
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(fixture))
                .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "true")
                .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                .build();
        launcher.getLauncher().execute(request, fixture == FirstFixture.class ? first : second);
      }
    }
    assertEquals(1, first.getSummary().getTestsFoundCount());
    assertEquals(1, first.getSummary().getTestsFailedCount());
    assertEquals(
        "intentional fixture failure",
        first.getSummary().getFailures().get(0).getException().getMessage());
    assertEquals(1, second.getSummary().getTestsFoundCount());
    assertEquals(
        1,
        second.getSummary().getTestsSucceededCount(),
        second.getSummary().getFailures().toString());
    assertEquals(2, clusters.size());
    assertSame(clusters.get(0), clusters.get(1));
    assertFalse(clusters.get(0).isRunning(), "launcher session must shut down its cluster");
  }

  static class CaptureCluster implements BeforeEachCallback {
    @Override
    public void beforeEach(ExtensionContext context) {
      clusters.add(SharedFlinkCluster.session(context).getMiniCluster());
    }
  }

  @ExtendWith(CaptureCluster.class)
  static class FirstFixture {
    @Test
    void leavesHostJobRunningOnFailure() throws Exception {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      assertInstanceOf(TestStreamEnvironment.class, env);
      env.setParallelism(1);
      StreamTestSinks.discard(env.fromSequence(0, Long.MAX_VALUE));
      unfinished = env.executeAsync("lifecycle-unfinished-job");
      var budget = new Configuration();
      budget.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1));
      TaskOffHeapMemory.initialize(budget);
      fail("intentional fixture failure");
    }
  }

  @ExtendWith(CaptureCluster.class)
  static class SecondFixture {
    @Test
    void executesFreshJobOnSameClusterAfterClassCleanup() throws Exception {
      assertSame(clusters.get(0), clusters.get(1));
      assertEquals(
          clusters
              .get(1)
              .getConfiguration()
              .get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY)
              .getBytes(),
          TaskOffHeapMemory.capacityBytes());
      assertEquals(JobStatus.CANCELED, unfinished.getJobStatus().get(10, TimeUnit.SECONDS));
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      assertInstanceOf(TestStreamEnvironment.class, env);
      env.setParallelism(1);
      List<Long> rows = new ArrayList<>();
      try (var iterator = env.fromSequence(1, 3).executeAndCollect()) {
        iterator.forEachRemaining(rows::add);
      }
      assertEquals(List.of(1L, 2L, 3L), rows);
    }
  }
}
