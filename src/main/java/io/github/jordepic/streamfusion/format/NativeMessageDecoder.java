package io.github.jordepic.streamfusion.format;

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

  void decodeInto(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress)
      throws Exception;

  @Override
  void close() throws Exception;
}
