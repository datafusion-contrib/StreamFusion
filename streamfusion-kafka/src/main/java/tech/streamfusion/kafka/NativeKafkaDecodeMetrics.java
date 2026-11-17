package tech.streamfusion.kafka;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * The native decode's work at one Kafka source reader, the source-side twin of the sink's
 * {@code nativeKafkaSerialization*} counters: the split batches decoded, the Kafka records they
 * carried, the rows they decoded to, and the time spent decoding them.
 */
final class NativeKafkaDecodeMetrics {

  private final Counter batches;
  private final Counter records;
  private final Counter rows;
  private final Counter nanos;

  NativeKafkaDecodeMetrics(MetricGroup group) {
    batches = group.counter("nativeKafkaDecodeBatches");
    records = group.counter("nativeKafkaDecodeRecords");
    rows = group.counter("nativeKafkaDecodeRows");
    nanos = group.counter("nativeKafkaDecodeNanos");
  }

  void decoded(int recordCount, int rowCount, long elapsedNanos) {
    batches.inc();
    records.inc(recordCount);
    rows.inc(rowCount);
    nanos.inc(elapsedNanos);
  }
}
