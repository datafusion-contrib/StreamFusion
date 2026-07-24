package io.github.jordepic.streamfusion.state;

/**
 * One restored checkpoint's Paimon table, materialized on local disk: the directory its files were
 * downloaded into and the snapshot id the checkpoint pinned. A restore may carry several sources
 * (rescale); the native side adopts from each the buckets in the operator's key-group range.
 */
public final class PaimonRestoredSource {

  private final String directory;
  private final long snapshotId;

  public PaimonRestoredSource(String directory, long snapshotId) {
    this.directory = directory;
    this.snapshotId = snapshotId;
  }

  public String directory() {
    return directory;
  }

  public long snapshotId() {
    return snapshotId;
  }
}
