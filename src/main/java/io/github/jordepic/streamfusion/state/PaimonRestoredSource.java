package io.github.jordepic.streamfusion.state;

/**
 * One restored checkpoint's Paimon state, materialized on local disk: the directory its files were
 * downloaded into and the opaque snapshot token the checkpoint's meta document carried (defined
 * and consumed only by the native store). A restore may carry several sources (rescale); the
 * native side adopts from each the buckets in the operator's key-group range.
 */
public final class PaimonRestoredSource {

  private final String directory;
  private final String snapshotToken;

  public PaimonRestoredSource(String directory, String snapshotToken) {
    this.directory = directory;
    this.snapshotToken = snapshotToken;
  }

  public String directory() {
    return directory;
  }

  public String snapshotToken() {
    return snapshotToken;
  }
}
