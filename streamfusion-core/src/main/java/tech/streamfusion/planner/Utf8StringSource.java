package tech.streamfusion.planner;

/** Optional contract for a source whose selected reader guarantees UTF-8 string bytes. */
public interface Utf8StringSource {
  /**
   * Returns true if every emitted CHAR and VARCHAR value, including nested values, contains valid
   * UTF-8. The contract applies after projection and other source abilities and covers all reader
   * modes available to this instance.
   */
  boolean guaranteesUtf8Strings();
}
