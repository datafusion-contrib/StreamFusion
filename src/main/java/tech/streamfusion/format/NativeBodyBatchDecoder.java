package tech.streamfusion.format;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;

import tech.streamfusion.operator.NativeAllocator;

/**
 * Task-local bridge from a binary body batch to a typed Arrow batch through one installed format
 * provider. It owns the Arrow C Data Interface round trip and the provider's {@code beforeDecode}
 * hook (schema-registry formats resolve writer schemas from the bodies there); callers own thread
 * placement.
 */
public final class NativeBodyBatchDecoder implements AutoCloseable {

  private final RowType outputType;
  private final NativeMessageDecoder decoder;
  private final BufferAllocator allocator;

  public NativeBodyBatchDecoder(
      NativeMessageDecoderFactory decoderFactory, RowType outputType, BufferAllocator allocator)
      throws Exception {
    this.outputType = outputType;
    this.allocator = allocator;
    this.decoder = decoderFactory.create();
    decoder.open(allocator, outputType);
  }

  /**
   * Decodes one body batch (a single binary column) into a typed batch, consuming (closing) the
   * input. The result can be empty — e.g. every document dropped by ignore-parse-errors — and is
   * owned by the caller.
   */
  public VectorSchemaRoot decode(VectorSchemaRoot bodies) throws Exception {
    try (VectorSchemaRoot in = bodies;
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decoder.beforeDecode((VarBinaryVector) in.getVector(0), in.getRowCount());
      Data.exportVectorSchemaRoot(allocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
      decoder.decodeInto(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      return Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
    }
  }

  /** Exercises the connector's contiguous Kafka-byte boundary without an input Arrow export. */
  public VectorSchemaRoot decodeContiguous(
      ArrowBuf data, long dataLength, long keyBytes, int[] lengths, int count, boolean keyed)
      throws Exception {
    if (!decoder.supportsContiguousBytes()) {
      throw new UnsupportedOperationException("decoder does not support contiguous byte batches");
    }
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decoder.decodeContiguousBytesInto(
          data.memoryAddress(),
          dataLength,
          keyBytes,
          lengths,
          count,
          keyed,
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      return Data.importVectorSchemaRoot(
          allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
    }
  }

  public RowType outputType() {
    return outputType;
  }

  /** The underlying format decoder — e.g. for a connector deciding whether it is C-ABI addressable. */
  public NativeMessageDecoder decoder() {
    return decoder;
  }

  @Override
  public void close() throws Exception {
    decoder.close();
  }
}
