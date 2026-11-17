package tech.streamfusion;

final class KafkaTestQuarantine {
  static final String REASON =
      "CI_QUARANTINE_LEGACY_KAFKA_TASK_FAILURE: intermittent extra committed record; "
          + "https://github.com/datafusion-contrib/StreamFusion/actions/runs/43655033296/job/132632169593";

  private KafkaTestQuarantine() {}

  static boolean matches(
      boolean enabled,
      boolean flink22,
      int jdk,
      String testClass,
      String mode,
      boolean ordinary,
      boolean nativeFirst,
      String naming) {
    return enabled
        && !flink22
        && jdk == 17
        && testClass.equals("tech.streamfusion.CanonicalKafkaPipelineFailureJobTest")
        && mode.equals("task-failure")
        && !ordinary
        && !nativeFirst
        && naming.equals("incrementing");
  }
}
