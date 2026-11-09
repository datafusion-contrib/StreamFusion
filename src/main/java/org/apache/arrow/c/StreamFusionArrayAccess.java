package org.apache.arrow.c;

/**
 * Uses Arrow's package-private import state to release only arrays not consumed by its importer.
 */
public final class StreamFusionArrayAccess {
  private StreamFusionArrayAccess() {}

  public static void releaseIfOpen(ArrowArray array) {
    if (!array.isClosed()) array.release();
  }
}
