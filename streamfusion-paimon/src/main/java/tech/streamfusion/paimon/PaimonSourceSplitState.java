package tech.streamfusion.paimon;

import org.apache.paimon.flink.source.FileStoreSourceSplit;

/** The emitted logical-row count, serialized with Paimon's existing split serializer. */
final class PaimonSourceSplitState {
  final FileStoreSourceSplit split;
  long emitted;

  PaimonSourceSplitState(FileStoreSourceSplit split) {
    this.split = split;
    this.emitted = split.recordsToSkip();
  }

  FileStoreSourceSplit checkpoint() {
    return split.updateWithRecordsToSkip(emitted);
  }
}
