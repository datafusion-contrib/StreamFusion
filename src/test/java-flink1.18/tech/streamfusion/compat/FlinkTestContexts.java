package tech.streamfusion.compat;

/** Leave the suite's local cluster contexts before submitting to a separate task JVM. */
public final class FlinkTestContexts {
  private FlinkTestContexts() {}

  public static void clearForRemoteSubmission() {
    org.apache.flink.test.util.TestEnvironment.unsetAsContext();
    org.apache.flink.streaming.util.TestStreamEnvironment.unsetAsContext();
  }
}
