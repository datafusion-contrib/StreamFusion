package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.TestEnvironment;
import org.junit.jupiter.api.Test;

class FlinkBatchTestContextLifecycleTest {
  @Test
  void batchLookupStartsTheClusterAndBindsStreaming() throws Exception {
    var batch = ExecutionEnvironment.getExecutionEnvironment();
    assertInstanceOf(TestEnvironment.class, batch);
    assertInstanceOf(TestStreamEnvironment.class, StreamExecutionEnvironment.getExecutionEnvironment());
    assertEquals(3, batch.fromElements(1, 2, 3).count());
  }

  @Test
  void streamingLookupBindsTheSameSessionsBatchEnvironment() throws Exception {
    assertInstanceOf(TestStreamEnvironment.class, StreamExecutionEnvironment.getExecutionEnvironment());
    var batch = ExecutionEnvironment.getExecutionEnvironment();
    assertInstanceOf(TestEnvironment.class, batch);
    assertEquals(3, batch.fromElements(1, 2, 3).count());
  }
}
