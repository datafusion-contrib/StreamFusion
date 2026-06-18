package io.github.jordepic.streamfusion;

/** Entry point to the native data plane. Holds the methods backed by the Rust library. */
public final class Native {

  static {
    System.loadLibrary("streamfusion");
  }

  private Native() {}

  /** Version reported by the loaded native library, proving the JVM↔Rust bridge is live. */
  public static native String version();

  /**
   * Sums an int32 column the JVM has exported through the Arrow C Data Interface.
   *
   * @param arrayAddress address of the producer-allocated {@code ArrowArray} C struct
   * @param schemaAddress address of the producer-allocated {@code ArrowSchema} C struct
   */
  public static native long sumInt(long arrayAddress, long schemaAddress);

  /**
   * Imports an int32 column the JVM exported and exports an equal column back into the
   * consumer-allocated C structs, exercising both directions of the boundary.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   */
  public static native void roundTrip(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);
}
