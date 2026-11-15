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

  static java.util.stream.Stream<String> kafkaNamingStrategies() {
    return java.util.stream.Stream.of("incrementing");
  }

  static String kafkaNamingOption(String naming) {
    if (!naming.equals("incrementing")) throw new IllegalArgumentException(naming);
    return "";
  }

  static String kafkaOwnership(String naming) {
    kafkaNamingOption(naming);
    return "LEGACY_SUBTASK_ID";
  }

  static String[] kafkaWriterState(Object writer, int subtask, int parallelism) throws Exception {
    // The 3.2 writer class is package-private; its only durable field is the prefix.
    var prefix = writer.getClass().getDeclaredMethod("getTransactionalIdPrefix");
    prefix.setAccessible(true);
    return new String[] {
      (String) prefix.invoke(writer), subtask + "/" + parallelism + "/LEGACY_SUBTASK_ID", "0"
    };
  }

  @SuppressWarnings("unchecked")
  static java.util.List<String> kafkaPendingCommittables(
      org.apache.flink.core.io.SimpleVersionedSerializer<?> serializer,
      byte[] bytes,
      int subtask,
      int parallelism)
      throws Exception {
    var collectorSerializer =
        new org.apache.flink.streaming.runtime.operators.sink.committables
            .CommittableCollectorSerializer<>(
            (org.apache.flink.core.io.SimpleVersionedSerializer<Object>) serializer,
            subtask,
            parallelism);
    var collector =
        org.apache.flink.core.io.SimpleVersionedSerialization.readVersionAndDeSerialize(
            collectorSerializer, bytes);
    var transactions = new java.util.ArrayList<String>();
    int expected = 0;
    // Released 1.18 exposes requests only within the runtime package. Inspect the decoded copy.
    for (var manager : collector.getCheckpointCommittablesUpTo(Long.MAX_VALUE)) {
      expected += manager.getSummary().getNumberOfPendingCommittables();
      var pending = manager.getClass().getDeclaredMethod("getPendingRequests", boolean.class);
      pending.setAccessible(true);
      var requests =
          (java.util.Collection<
                  org.apache.flink.api.connector.sink2.Committer.CommitRequest<Object>>)
              pending.invoke(manager, false);
      for (var request : requests) {
        Object committable = request.getCommittable();
        var transaction = committable.getClass().getDeclaredMethod("getTransactionalId");
        transaction.setAccessible(true);
        transactions.add((String) transaction.invoke(committable));
      }
    }
    if (transactions.size() != expected)
      throw new java.io.IOException("Incomplete released Kafka committable inventory");
    return transactions;
  }

  static String kafkaFailureEvidence() {
    return "FENCED_COMMITTABLE_STATE";
  }
}
