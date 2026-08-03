package tech.streamfusion.fluss;

import java.util.OptionalLong;

/** Concrete log-table split assignment passed from Flink's Fluss enumerator to the native reader. */
public final class NativeFlussLogSplit {

  private final String splitId;
  private final long tableId;
  private final Long partitionId;
  private final int bucket;
  private final long startingOffset;
  private final Long stoppingOffset;

  NativeFlussLogSplit(
      String splitId,
      long tableId,
      Long partitionId,
      int bucket,
      long startingOffset,
      Long stoppingOffset) {
    this.splitId = splitId;
    this.tableId = tableId;
    this.partitionId = partitionId;
    this.bucket = bucket;
    this.startingOffset = startingOffset;
    this.stoppingOffset = stoppingOffset;
  }

  public String splitId() {
    return splitId;
  }

  public long tableId() {
    return tableId;
  }

  public OptionalLong partitionId() {
    return partitionId == null ? OptionalLong.empty() : OptionalLong.of(partitionId);
  }

  public int bucket() {
    return bucket;
  }

  public long startingOffset() {
    return startingOffset;
  }

  public OptionalLong stoppingOffset() {
    return stoppingOffset == null ? OptionalLong.empty() : OptionalLong.of(stoppingOffset);
  }
}
