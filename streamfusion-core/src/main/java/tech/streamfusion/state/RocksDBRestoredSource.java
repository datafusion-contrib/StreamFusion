package tech.streamfusion.state;

/**
 * One restored checkpoint's RocksDB state, materialized on local disk: the directory its files were
 * downloaded into and the opaque snapshot token the checkpoint's meta document carried (defined
 * and consumed only by the native store), plus the key-group range the source subtask covered —
 * a single source covering exactly this subtask's range restores by wholesale file adoption,
 * anything else (rescale) restores by a key-group-range clip rewrite.
 */
public final class RocksDBRestoredSource {

  private final String directory;
  private final String snapshotToken;
  private final int keyGroupStart;
  private final int keyGroupEnd;

  public RocksDBRestoredSource(
      String directory, String snapshotToken, int keyGroupStart, int keyGroupEnd) {
    this.directory = directory;
    this.snapshotToken = snapshotToken;
    this.keyGroupStart = keyGroupStart;
    this.keyGroupEnd = keyGroupEnd;
  }

  public String directory() {
    return directory;
  }

  public String snapshotToken() {
    return snapshotToken;
  }

  public int keyGroupStart() {
    return keyGroupStart;
  }

  public int keyGroupEnd() {
    return keyGroupEnd;
  }
}
