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

  @SuppressWarnings("unchecked")
  static java.util.List<String> kafkaPendingCommittables(
      org.apache.flink.core.io.SimpleVersionedSerializer<?> serializer,
      byte[] bytes,
      int subtask,
      int parallelism)
      throws Exception {
    var metrics =
        org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup.wrap(
            org.apache.flink.metrics.groups.UnregisteredMetricsGroup.createOperatorMetricGroup());
    var collectorSerializer =
        new org.apache.flink.streaming.runtime.operators.sink.committables
            .CommittableCollectorSerializer<>(
            (org.apache.flink.core.io.SimpleVersionedSerializer<Object>) serializer,
            subtask,
            parallelism,
            metrics);
    var collector =
        org.apache.flink.core.io.SimpleVersionedSerialization.readVersionAndDeSerialize(
            collectorSerializer, bytes);
    var transactions = new java.util.ArrayList<String>();
    // The released runtime exposes pending requests only within its package. Read the decoded copy.
    for (var manager : collector.getCheckpointCommittablesUpTo(Long.MAX_VALUE)) {
      var pending = manager.getClass().getDeclaredMethod("getPendingRequests");
      pending.setAccessible(true);
      try (var requests =
          (java.util.stream.Stream<
                  org.apache.flink.api.connector.sink2.Committer.CommitRequest<Object>>)
              pending.invoke(manager)) {
        requests.forEach(
            request ->
                transactions.add(
                    ((org.apache.flink.connector.kafka.sink.KafkaCommittable)
                            request.getCommittable())
                        .getTransactionalId()));
      }
    }
    return transactions;
  }

  static String kafkaFailureEvidence() {
    return "FAILED_COMMITTABLE_METRIC";
  }
}
