package io.github.jordepic.streamfusion.kafka;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.kafka.common.TopicPartition;

/** Flink-facing metrics updated from native poll metadata without a rowwise JVM hot path. */
final class NativeKafkaSourceMetrics {

  private final KafkaSourceReaderMetrics kafkaMetrics;
  private final SourceReaderMetricGroup sourceMetrics;
  private final Set<TopicPartition> registered = ConcurrentHashMap.newKeySet();
  private final ConcurrentMap<TopicPartition, Long> recordsLag = new ConcurrentHashMap<>();
  private final AtomicLong bytesConsumed = new AtomicLong();
  private final AtomicLong recordsConsumed = new AtomicLong();

  NativeKafkaSourceMetrics(SourceReaderMetricGroup sourceMetrics) {
    this.sourceMetrics = sourceMetrics;
    this.kafkaMetrics = new KafkaSourceReaderMetrics(sourceMetrics);
    sourceMetrics.setPendingRecordsGauge(
        () -> recordsLag.values().stream().mapToLong(Long::longValue).sum());
    MetricGroup consumer =
        sourceMetrics.addGroup("KafkaSourceReader").addGroup("KafkaConsumer");
    consumer.gauge("bytes-consumed-total", bytesConsumed::get);
    consumer.gauge("records-consumed-total", recordsConsumed::get);
  }

  void register(TopicPartition partition) {
    if (registered.add(partition)) {
      kafkaMetrics.registerTopicPartition(partition);
    }
  }

  void recordPoll(
      TopicPartition partition, long nextOffset, long bytes, long records, long highWatermark) {
    register(partition);
    kafkaMetrics.recordCurrentOffset(partition, Math.max(-1, nextOffset - 1));
    sourceMetrics.getIOMetricGroup().getNumBytesInCounter().inc(bytes);
    bytesConsumed.addAndGet(bytes);
    recordsConsumed.addAndGet(records);
    if (highWatermark >= 0) {
      recordsLag.put(partition, Math.max(0, highWatermark - nextOffset));
    }
  }

  void recordCommit(TopicPartition partition, long offset) {
    register(partition);
    kafkaMetrics.recordCommittedOffset(partition, offset);
  }

  void recordSucceededCommit() {
    kafkaMetrics.recordSucceededCommit();
  }

  void recordFailedCommit() {
    kafkaMetrics.recordFailedCommit();
  }
}
