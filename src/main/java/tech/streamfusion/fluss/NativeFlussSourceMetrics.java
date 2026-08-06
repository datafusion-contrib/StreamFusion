package tech.streamfusion.fluss;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.metadata.TableBucket;

/** Bridges native Fluss poll metadata into Fluss' own Flink metric surface. */
final class NativeFlussSourceMetrics {

  private final SourceReaderMetricGroup sourceMetrics;
  private final FlinkSourceReaderMetrics flussMetrics;
  private final Map<String, TableBucket> buckets = new ConcurrentHashMap<>();

  NativeFlussSourceMetrics(SourceReaderMetricGroup sourceMetrics) {
    this.sourceMetrics = sourceMetrics;
    this.flussMetrics = new FlinkSourceReaderMetrics(sourceMetrics);
  }

  void register(NativeFlussLogSplit split) {
    buckets.computeIfAbsent(
        split.splitId(),
        ignored -> {
          TableBucket bucket =
              split.partitionId().isPresent()
                  ? new TableBucket(split.tableId(), split.partitionId().getAsLong(), split.bucket())
                  : new TableBucket(split.tableId(), split.bucket());
          flussMetrics.registerTableBucket(bucket);
          return bucket;
        });
  }

  void recordBatch(String splitId, long nextOffset, int records, long maxEventTime) {
    TableBucket bucket = buckets.get(splitId);
    if (bucket != null) {
      flussMetrics.recordCurrentOffset(bucket, Math.max(-1, nextOffset - 1));
    }
    if (records > 1) {
      // SourceReaderBase accounts the NativeSourceRecord wrapper itself.
      sourceMetrics.getIOMetricGroup().getNumRecordsInCounter().inc(records - 1L);
    }
    if (maxEventTime > 0) {
      flussMetrics.reportRecordEventTime(Math.max(0, System.currentTimeMillis() - maxEventTime));
    }
  }
}
