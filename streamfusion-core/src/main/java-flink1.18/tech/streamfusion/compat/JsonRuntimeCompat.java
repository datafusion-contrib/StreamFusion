package tech.streamfusion.compat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;

/** The buffer-recycler contract verified for the line's released Jackson runtime. */
public final class JsonRuntimeCompat {
  private JsonRuntimeCompat() {}

  public static final boolean LEGACY_SQL_JSON = true;

  public static final boolean ACCEPTS_ARRAY_ROOTS = false;

  /**
   * Whether the JSON format's collector path hands its collector a null row for an object it
   * skips under {@code ignore-parse-errors} (the parser path's array-capable deserialize). The
   * single-row deserialize of earlier lines returns that null instead, and it is dropped.
   */
  public static final boolean COLLECTS_SKIPPED_OBJECTS_AS_NULL = false;

  public static final boolean PRESERVES_DECIMAL_SCALE = false;

  public static boolean verifiedFactory(JsonFactory factory) {
    var version = factory.version();
    return version.getMajorVersion() == 2
        && version.getMinorVersion() == 14
        && version.getPatchLevel() == 2
        && (Runtime.version().feature() == 11 || Runtime.version().feature() == 17)
        && factory.isEnabled(JsonFactory.Feature.USE_THREAD_LOCAL_FOR_BUFFER_RECYCLING);
  }

  public static void releaseToPool(BufferRecycler recycler) {
    // Jackson 2.14 keeps its recycler in a thread-local SoftReference; no pool release API.
  }
}
