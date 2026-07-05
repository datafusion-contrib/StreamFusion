package io.github.jordepic.streamfusion.fluss;

import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;

/** Converts Fluss's public source splits into the minimal native log-reader assignment. */
public final class FlussSplitTranslator {

  private FlussSplitTranslator() {}

  public static boolean isNativeLogSplit(SourceSplitBase split) {
    return split.isLogSplit();
  }

  public static NativeFlussLogSplit translateLogSplit(SourceSplitBase split) {
    if (!split.isLogSplit()) {
      throw new IllegalArgumentException(
          "native Fluss proof path only accepts log splits, got " + split.getClass().getSimpleName());
    }
    LogSplit log = split.asLogSplit();
    TableBucket bucket = log.getTableBucket();
    return new NativeFlussLogSplit(
        log.splitId(),
        bucket.getTableId(),
        bucket.getPartitionId(),
        log.getPartitionName(),
        bucket.getBucket(),
        log.getStartingOffset(),
        log.getStoppingOffset().orElse(null));
  }
}
