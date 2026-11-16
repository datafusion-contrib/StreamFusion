package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tech.streamfusion.compat.StreamTestSinks;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SharedFlinkClusterInvocationCleanupTest {
  private static org.apache.flink.core.execution.JobClient previous;

  @Test
  @Order(1)
  void aTestMayLeaveAnUnboundedJobForTheFixtureToClose() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var session = SharedFlinkClusterSession.currentCluster();
    assertNotNull(session);
    env.setParallelism(session.getNumberSlots());
    StreamTestSinks.discard(
        env.fromSequence(0, Long.MAX_VALUE)
            .map(value -> {
              Thread.sleep(10);
              return value;
            }));
    previous = env.executeAsync("fixture-cleanup-regression");
    CommonTestUtils.waitForAllTaskRunning(session.getMiniCluster(), previous.getJobID(), false);
    assertEquals(
        0,
        session.getMiniCluster()
            .getResourceOverview()
            .get(10, TimeUnit.SECONDS)
            .getNumberFreeSlots(),
        "The unfinished invocation must occupy every slot");
  }

  @Test
  @Order(2)
  void theNextInvocationStartsAfterThePreviousJobReleasedItsSlots() throws Exception {
    assertNotNull(previous);
    assertEquals(JobStatus.CANCELED, previous.getJobStatus().get(10, TimeUnit.SECONDS));
    var session = SharedFlinkClusterSession.currentCluster();
    assertNotNull(session);
    assertEquals(
        session.getNumberSlots(),
        session.getMiniCluster()
            .getResourceOverview()
            .get(10, TimeUnit.SECONDS)
            .getNumberFreeSlots());
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(session.getNumberSlots());
    try (var rows = env.fromSequence(1, 1).executeAndCollect()) {
      assertTrue(rows.hasNext());
      assertEquals(1L, rows.next());
      assertFalse(rows.hasNext());
    }
  }
}
