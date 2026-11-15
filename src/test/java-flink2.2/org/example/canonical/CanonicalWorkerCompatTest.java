package org.example.canonical;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;
import org.junit.jupiter.api.Test;

class CanonicalWorkerCompatTest {
  @Test
  void retainsEveryPendingTransactionAcrossCheckpointsAndRescaledWriters() throws Exception {
    var metrics =
        InternalSinkCommitterMetricGroup.wrap(UnregisteredMetricsGroup.createOperatorMetricGroup());
    var collector = new CommittableCollector<KafkaCommittable>(metrics);
    var expected = new ArrayList<String>();
    for (long checkpoint : List.of(7L, 8L)) {
      for (int subtask = 0; subtask < 3; subtask++) {
        String transaction = "transaction-" + checkpoint + "-" + subtask;
        collector.addMessage(new CommittableSummary<>(subtask, 3, checkpoint, 1, 1, 0));
        collector.addMessage(
            new CommittableWithLineage<>(
                new KafkaCommittable(100 + subtask, (short) 1, transaction, null),
                checkpoint,
                subtask));
        expected.add(transaction);
      }
    }
    var serializer = serializer();
    var stateSerializer = new CommittableCollectorSerializer<>(serializer, 0, 3, metrics);
    byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(stateSerializer, collector);
    var actual =
        new ArrayList<>(CanonicalWorkerCompat.kafkaPendingCommittables(serializer, bytes, 0, 3));
    expected.sort(String::compareTo);
    actual.sort(String::compareTo);
    assertEquals(expected, actual);
  }

  @Test
  void emptyStateStaysEmptyAndCorruptionDoesNotBecomeAnEmptyInventory() throws Exception {
    var metrics =
        InternalSinkCommitterMetricGroup.wrap(UnregisteredMetricsGroup.createOperatorMetricGroup());
    var serializer = serializer();
    var stateSerializer = new CommittableCollectorSerializer<>(serializer, 0, 3, metrics);
    byte[] bytes =
        SimpleVersionedSerialization.writeVersionAndSerialize(
            stateSerializer, new CommittableCollector<KafkaCommittable>(metrics));
    assertEquals(List.of(), CanonicalWorkerCompat.kafkaPendingCommittables(serializer, bytes, 0, 3));
    assertThrows(
        java.io.IOException.class,
        () ->
            CanonicalWorkerCompat.kafkaPendingCommittables(
                serializer, new byte[] {0, 0, 0, 99, 0, 0, 0, 0}, 0, 3));
  }

  private static org.apache.flink.core.io.SimpleVersionedSerializer<KafkaCommittable> serializer() {
    return KafkaSink.<String>builder()
        .setBootstrapServers("unused:9092")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic("unused")
                .setValueSerializationSchema(
                    new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build())
        .build()
        .getCommittableSerializer();
  }
}
