package tech.streamfusion.format;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.flink.table.types.logical.RowType;

/**
 * Task-local bridge to one native format library. The generic batching operator owns Arrow C Data
 * Interface export/import and calls this implementation only for format-specific work.
 */
public interface NativeMessageDecoder extends AutoCloseable {

  void open(BufferAllocator allocator, RowType outputType) throws Exception;

  default void beforeDecode(VarBinaryVector bodies, int count) throws Exception {}

  /**
   * The exported driver init of this decoder's native library, or 0 when the decode cannot be
   * invoked without JVM mediation (the default — a decoder must opt in, and one that needs
   * per-batch JVM work such as {@link #beforeDecode} must not). A connector calls the init with the
   * ABI version it speaks (the ADBC driver-init pattern); the format fills the decode vtable or
   * refuses, and a refusal falls back to the JVM-mediated decode.
   */
  default long driverInitAddress() {
    return 0;
  }

  /** The opaque native decoder handle the driver's decode is invoked with; 0 when not addressable. */
  default long decoderHandle() {
    return 0;
  }

  /** Whether this decoder can consume one contiguous Kafka byte slab without Arrow input export. */
  default boolean supportsContiguousBytes() {
    return false;
  }

  /**
   * Decodes Kafka records stored as {@code [all keys][all values]}. {@code lengths} contains the
   * key and value length for each record in that order; {@code -1} denotes a null. Implementations
   * opting into this boundary avoid constructing and exporting input {@code VarBinaryVector}s.
   */
  default void decodeContiguousBytesInto(
      long dataAddress,
      long dataLength,
      long keyBytes,
      int[] lengths,
      int count,
      boolean keyed,
      long outArrayAddress,
      long outSchemaAddress)
      throws Exception {
    throw new UnsupportedOperationException("decoder does not support contiguous byte batches");
  }

  void decodeInto(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress)
      throws Exception;

  @Override
  void close() throws Exception;
}
