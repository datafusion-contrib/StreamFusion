package org.example.canonical;

final class CanonicalWorkerCompat {
  private CanonicalWorkerCompat() {}

  static String operatorName(org.apache.flink.runtime.checkpoint.OperatorState state) {
    return "";
  }

  static String plan(org.apache.flink.runtime.executiongraph.AccessExecutionGraph graph) {
    return graph.getJsonPlan();
  }

  public static String storage(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env) {
    return String.valueOf(env.getCheckpointConfig().getCheckpointStorage());
  }
}
