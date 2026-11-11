package org.example.canonical;

final class CanonicalWorkerCompat {
  private CanonicalWorkerCompat() {}

  static String operatorName(org.apache.flink.runtime.checkpoint.OperatorState state) {
    return state.getOperatorName().orElse("");
  }

  static String plan(org.apache.flink.runtime.executiongraph.AccessExecutionGraph graph) {
    return graph.getStreamGraphJson();
  }

  public static String storage(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env) {
    return "configured";
  }

  static java.util.stream.Stream<String> kafkaNamingStrategies() {
    return java.util.stream.Stream.of("incrementing", "pooling");
  }

  static String kafkaNamingOption(String naming) {
    return ",'sink.transaction-naming-strategy'='" + naming + "'";
  }

  static String kafkaOwnership(String naming) {
    return naming.equals("pooling") ? "EXPLICIT_BY_WRITER_STATE" : "IMPLICIT_BY_SUBTASK_ID";
  }

  static String[] kafkaWriterState(Object state, int subtask, int parallelism) {
    var writer = (org.apache.flink.connector.kafka.sink.KafkaWriterState) state;
    return new String[] {
      writer.getTransactionalIdPrefix(),
      writer.getOwnedSubtaskId()
          + "/"
          + writer.getTotalNumberOfOwnedSubtasks()
          + "/"
          + writer.getTransactionOwnership(),
      Integer.toString(writer.getPrecommittedTransactionalIds().size())
    };
  }

  static java.util.List<String> kafkaPendingCommittables(
      org.apache.flink.core.io.SimpleVersionedSerializer<?> serializer,
      byte[] bytes,
      int subtask,
      int parallelism) {
    return java.util.List.of();
  }

  static String kafkaFailureEvidence() {
    return "FAILED_COMMITTABLE_METRIC";
  }
}
