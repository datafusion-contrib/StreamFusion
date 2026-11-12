package tech.streamfusion.compat;

/** Bind local test contexts and clear them before remote submission. */
public final class FlinkTestContexts {
  private FlinkTestContexts() {}

  public static void deferForLocalTests(Runnable initialize) {
    LazyBatchContext.install(initialize);
    LazyStreamContext.install(initialize);
  }

  private static final class LazyStreamContext
      extends org.apache.flink.streaming.util.TestStreamEnvironment {
    private LazyStreamContext() {
      super(null, 1);
    }

    static void install(Runnable initialize) {
      initializeContextEnvironment(
          configuration -> {
            initialize.run();
            // Initialization replaces this deferred factory with Flink's ordinary test factory.
            return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                .getExecutionEnvironment(configuration);
          });
    }
  }

  private static final class LazyBatchContext extends org.apache.flink.test.util.TestEnvironment {
    private LazyBatchContext() {
      super(null, 1, false);
    }

    static void install(Runnable initialize) {
      initializeContextEnvironment(
          () -> {
            initialize.run();
            return org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
          });
    }
  }

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
