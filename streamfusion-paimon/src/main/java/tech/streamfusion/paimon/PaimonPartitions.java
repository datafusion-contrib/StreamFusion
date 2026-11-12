package tech.streamfusion.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.memory.MemorySegment;

/** Re-wraps the partition bytes the native router produced as Paimon's partition row. */
final class PaimonPartitions {
  private PaimonPartitions() {}

  static BinaryRow fromBytes(byte[] bytes, int arity) {
    BinaryRow partition = new BinaryRow(arity);
    partition.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
    return partition;
  }
}
