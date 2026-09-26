package tech.streamfusion.parquet;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry points for optional native Parquet encoding and decoding. */
public final class NativeParquet {

  static {
    NativeExtensionLoader.load(
        NativeParquet.class,
        "parquet",
        NativeParquet::nativeBuildVersion,
        NativeParquet::liveNativeHandles);
  }

  private NativeParquet() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Live handles owned by this library, used by the shared leak sentinel. */
  public static native String liveNativeHandles();

  /** Forces initialization of the extension class, including loading its native library. */
  public static boolean isLoaded() {
    return true;
  }

  public static native long createParquetDecoder(
      Object input, long length, long schemaAddress, String[] physicalNames, int batchSize);

  public static native boolean parquetDecoderNext(long handle, long arrayAddress, long schemaAddress);

  public static native void closeParquetDecoder(long handle);

  public static native long parquetDecoderMaxRowGroupBytes(long handle);

  /** Flink's local-calendar INT96 conversion, evaluated once per Arrow timestamp column. */
  public static byte[] localInt96(long[] parts) {
    var bytes =
        java.nio.ByteBuffer.allocate(Math.multiplyExact(parts.length / 2, 12))
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
    for (int index = 0; index < parts.length; index += 2) {
      var timestamp =
          org.apache.flink.table.data.TimestampData.fromEpochMillis(
                  parts[index], (int) parts[index + 1])
              .toTimestamp();
      long millis = timestamp.getTime();
      long nanos = ((millis % 86_400_000L) / 1_000L) * 1_000_000_000L + timestamp.getNanos();
      bytes.putLong(nanos).putInt((int) (millis / 86_400_000L + 2_440_588L));
    }
    return bytes.array();
  }

  public static native long createParquetEncoder(
      long schemaAddress,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues,
      boolean changelog,
      Object output,
      byte[] chunk);

  public static native void parquetEncoderWrite(
      long handle, long inArrayAddress, int[] selectedRows, int rowOffset, int rowCount);

  public static native long parquetEncoderEstimatedBytes(long handle);

  public static native void parquetEncoderFinish(long handle);

  public static native void closeParquetEncoder(long handle);

  public static native long splitByPartitionColumns(
      long inArrayAddress, long inSchemaAddress, int[] partitionColumns);

  public static native boolean nextPartitionSlice(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native void closePartitionSplit(long handle);

}
