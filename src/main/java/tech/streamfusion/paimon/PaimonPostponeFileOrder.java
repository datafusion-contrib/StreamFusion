package tech.streamfusion.paimon;

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

/** Paimon replays a writer's postpone files by their millisecond creation time. */
final class PaimonPostponeFileOrder implements Supplier<Timestamp> {
  private final LongSupplier clock;
  private long last;

  PaimonPostponeFileOrder(FileStoreTable table, BinaryRow partition, String prefix) {
    this(table, partition, prefix, () -> Timestamp.now().getMillisecond());
  }

  PaimonPostponeFileOrder(
      FileStoreTable table, BinaryRow partition, String prefix, LongSupplier clock) {
    this.clock = clock;
    this.last = Long.MIN_VALUE;
    for (ManifestEntry entry :
        table
            .store()
            .newScan()
            .withPartitionBucket(partition, BucketMode.POSTPONE_BUCKET)
            .plan()
            .files()) {
      if (entry.file().fileName().startsWith(prefix)) {
        last = Math.max(last, entry.file().creationTime().getMillisecond());
      }
    }
  }

  @Override
  public Timestamp get() {
    // Footer extraction can describe several rolled files in the same millisecond. Scan order is
    // not their input order, so equal timestamps would let older changes overwrite newer ones.
    last = Math.max(clock.getAsLong(), last + 1);
    return Timestamp.fromEpochMillis(last);
  }
}
