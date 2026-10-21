package tech.streamfusion.compat;

/** Bind local test contexts and clear them before remote submission. */
public final class FlinkTestContexts {
  private FlinkTestContexts() {}

  public static void setForLocalTests(
      org.apache.flink.runtime.minicluster.MiniCluster cluster, int parallelism) {
    org.apache.flink.test.util.TestEnvironment.setAsContext(cluster, parallelism);
    org.apache.flink.streaming.util.TestStreamEnvironment.setAsContext(cluster, parallelism);
  }

  public static void clearForRemoteSubmission() {
    org.apache.flink.test.util.TestEnvironment.unsetAsContext();
    org.apache.flink.streaming.util.TestStreamEnvironment.unsetAsContext();
  }
}
