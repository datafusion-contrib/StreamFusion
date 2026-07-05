package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.junit.jupiter.api.Test;

class FlussSplitTranslatorTest {

  @Test
  void extractsNativeAssignmentFromNonPartitionedLogSplit() {
    LogSplit split = new LogSplit(new TableBucket(7L, 2), null, 11L, 42L);

    NativeFlussLogSplit nativeSplit = FlussSplitTranslator.translateLogSplit(split);

    assertEquals(split.splitId(), nativeSplit.splitId());
    assertEquals(7L, nativeSplit.tableId());
    assertFalse(nativeSplit.partitionId().isPresent());
    assertEquals(2, nativeSplit.bucket());
    assertEquals(11L, nativeSplit.startingOffset());
    assertEquals(42L, nativeSplit.stoppingOffset().orElseThrow());
  }

  @Test
  void extractsNativeAssignmentFromPartitionedLogSplit() {
    LogSplit split = new LogSplit(new TableBucket(7L, 99L, 3), "dt=2026-07-04", 5L);

    NativeFlussLogSplit nativeSplit = FlussSplitTranslator.translateLogSplit(split);

    assertEquals(7L, nativeSplit.tableId());
    assertEquals(99L, nativeSplit.partitionId().orElseThrow());
    assertEquals("dt=2026-07-04", nativeSplit.partitionName());
    assertEquals(3, nativeSplit.bucket());
    assertEquals(5L, nativeSplit.startingOffset());
    assertFalse(nativeSplit.stoppingOffset().isPresent());
  }

  @Test
  void rejectsHybridSnapshotLogSplitForTheLogTableProofPath() {
    HybridSnapshotLogSplit split =
        new HybridSnapshotLogSplit(new TableBucket(7L, 2), null, 123L, 4L);

    assertFalse(FlussSplitTranslator.isNativeLogSplit(split));
    assertThrows(IllegalArgumentException.class, () -> FlussSplitTranslator.translateLogSplit(split));
  }
}
