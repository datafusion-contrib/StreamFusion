package tech.streamfusion.parquet;

import org.apache.flink.core.fs.FSDataOutputStream;
import tech.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Parquet sink. */
public final class NativeParquet {

  static {
    NativeExtensionLoader.load(NativeParquet.class, "parquet", NativeParquet::nativeBuildVersion);
  }

  private NativeParquet() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Forces initialization of the extension class, including loading its native library. */
  public static boolean isLoaded() {
    return true;
  }

  public static native long createParquetEncoder(
      long schemaAddress,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues,
      boolean changelog,
      FSDataOutputStream output,
      byte[] chunk);

  public static native void parquetEncoderWrite(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void parquetEncoderFinish(long handle);

  public static native void closeParquetEncoder(long handle);

  public static native long splitByPartitionColumns(
      long inArrayAddress, long inSchemaAddress, int[] partitionColumns);

  public static native boolean nextPartitionSlice(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native void closePartitionSplit(long handle);

}
