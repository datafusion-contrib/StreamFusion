package tech.streamfusion.compat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;

/** The buffer-recycler contract verified for the line's released Jackson runtime. */
public final class JsonRuntimeCompat {
  private JsonRuntimeCompat() {}

  public static final boolean LEGACY_SQL_JSON = false;

  public static final boolean ACCEPTS_ARRAY_ROOTS = true;

  public static final boolean PRESERVES_DECIMAL_SCALE = true;

  public static boolean verifiedFactory(JsonFactory factory) {
    var version = factory.version();
    return version.getMajorVersion() == 2
        && version.getMinorVersion() == 18
        && version.getPatchLevel() == 2
        && factory._getRecyclerPool()
            instanceof
            org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.JsonRecyclerPools
                .ThreadLocalPool;
  }

  public static void releaseToPool(BufferRecycler recycler) {
    recycler.releaseToPool();
  }
}
