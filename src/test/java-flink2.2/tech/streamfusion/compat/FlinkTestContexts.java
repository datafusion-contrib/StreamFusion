package tech.streamfusion.compat;

/** Leave the suite's local cluster context before submitting to a separate task JVM. */
public final class FlinkTestContexts {
  private FlinkTestContexts() {}

  public static void clearForRemoteSubmission() {
    org.apache.flink.streaming.util.TestStreamEnvironment.unsetAsContext();
  }
}
