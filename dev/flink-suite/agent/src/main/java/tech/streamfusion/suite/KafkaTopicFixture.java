package tech.streamfusion.suite;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/** Waits for the administrative path used by the host exactly-once sink after topic creation. */
public final class KafkaTopicFixture {
  private KafkaTopicFixture() {}

  public static void awaitReady(ClassLoader loader, String topic, Properties properties)
      throws Exception {
    Class<?> adminType = loader.loadClass("org.apache.kafka.clients.admin.Admin");
    Properties readinessProperties = new Properties();
    readinessProperties.putAll(properties);
    readinessProperties.setProperty("default.api.timeout.ms", "5000");
    readinessProperties.setProperty("request.timeout.ms", "5000");
    Object admin = adminType.getMethod("create", Properties.class).invoke(null, readinessProperties);
    try {
      var metadata =
          loader
              .loadClass("org.apache.flink.connector.kafka.util.AdminUtils")
              .getMethod("getProducerStates", adminType, java.util.Collection.class);
      awaitReady(() -> metadata.invoke(null, admin, List.of(topic)), Duration.ofSeconds(30), topic);
    } finally {
      adminType.getMethod("close", Duration.class).invoke(admin, Duration.ofSeconds(5));
    }
  }

  @FunctionalInterface
  interface Probe {
    void check() throws Exception;
  }

  static void awaitReady(Probe probe, Duration timeout, String topic) throws Exception {
    long start = System.nanoTime();
    while (true) {
      try {
        probe.check();
        return;
      } catch (Exception error) {
        Throwable cause = error;
        boolean transientMetadata = false;
        while (cause != null) {
          if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw (InterruptedException) cause;
          }
          String name = cause.getClass().getName();
          transientMetadata |=
              name.equals("org.apache.kafka.common.errors.UnknownTopicOrPartitionException")
                  || name.equals("org.apache.kafka.common.errors.LeaderNotAvailableException");
          cause = cause.getCause();
        }
        if (!transientMetadata) {
          if (error instanceof InvocationTargetException invocation
              && invocation.getCause() instanceof Exception underlying) {
            throw underlying;
          }
          throw error;
        }
        if (System.nanoTime() - start >= timeout.toNanos()) {
          var failure = new TimeoutException("Kafka administrative metadata is not ready for " + topic);
          failure.initCause(error);
          throw failure;
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw interrupted;
        }
      }
    }
  }
}
