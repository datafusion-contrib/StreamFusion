package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.streamfusion.compat.StreamTestSinks;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SharedFlinkClusterInvocationCleanupTest {
  private static org.apache.flink.core.execution.JobClient previous;

  @Test
  @Order(1)
  void aTestMayLeaveAnUnboundedJobForTheFixtureToClose() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTestSinks.discard(env.fromSequence(0, Long.MAX_VALUE));
    previous = env.executeAsync("fixture-cleanup-regression");
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    JobStatus status;
    do {
      status = previous.getJobStatus().get(10, TimeUnit.SECONDS);
      if (status == JobStatus.RUNNING) break;
      Thread.sleep(20);
    } while (System.nanoTime() < deadline);
    assertEquals(JobStatus.RUNNING, status);
  }

  @Test
  @Order(2)
  void theNextInvocationStartsAfterThePreviousJobReleasedItsSlots() throws Exception {
    assertNotNull(previous);
    assertEquals(JobStatus.CANCELED, previous.getJobStatus().get(10, TimeUnit.SECONDS));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    try (var rows = env.fromSequence(1, 1).executeAndCollect()) {
      assertTrue(rows.hasNext());
      assertEquals(1L, rows.next());
      assertFalse(rows.hasNext());
    }
  }
}
