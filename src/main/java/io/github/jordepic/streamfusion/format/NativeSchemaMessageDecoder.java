package io.github.jordepic.streamfusion.format;

import io.github.jordepic.streamfusion.operator.NativeAllocator;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;

/** Common schema-export plumbing for formats whose native decoder is driven by the output Arrow schema. */
public abstract class NativeSchemaMessageDecoder implements NativeMessageDecoder {

  @FunctionalInterface
  interface HandleCreator {
    long create(long schemaArrayAddress, long schemaAddress);
  }

  protected long handle;

  @Override
  public final void open(BufferAllocator allocator, RowType outputType) {
    try (VectorSchemaRoot template = RowDataArrowConverter.write(List.of(), outputType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, template, NativeAllocator.DICTIONARIES, array, schema);
      handle = createHandle(array.memoryAddress(), schema.memoryAddress());
    }
  }

  protected abstract long createHandle(long schemaArrayAddress, long schemaAddress);

  @Override
  public final long decoderHandle() {
    return handle;
  }
}
