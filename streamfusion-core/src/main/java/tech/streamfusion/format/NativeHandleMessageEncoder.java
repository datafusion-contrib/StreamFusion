package tech.streamfusion.format;

import java.util.function.LongConsumer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.operator.NativeAllocator;

/** C Data ownership stays with the importing format library; only JVM-owned bytes return. */
public final class NativeHandleMessageEncoder implements NativeMessageEncoder {
  @FunctionalInterface
  public interface Encode {
    EncodedMessages apply(long handle, long array, long schema) throws Exception;
  }

  private long handle;
  private final Encode encode;
  private final LongConsumer close;

  private NativeHandleMessageEncoder(Encode encode, LongConsumer close) {
    this.encode = encode;
    this.close = close;
  }

  /** Allocate the Java owner and callbacks before acquiring the native handle. */
  public static NativeHandleMessageEncoder open(
      java.util.function.LongSupplier create, Encode encode, LongConsumer close) {
    NativeHandleMessageEncoder owner = new NativeHandleMessageEncoder(encode, close);
    owner.handle = create.getAsLong();
    if (owner.handle == 0) throw new IllegalStateException("native encoder was not created");
    return owner;
  }

  @Override
  public EncodedMessages encode(VectorSchemaRoot input) throws Exception {
    if (handle == 0) throw new IllegalStateException("encoder is closed");
    var allocator =
        input.getFieldVectors().isEmpty()
            ? NativeAllocator.SHARED
            : input.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        AutoCloseable exports =
            () ->
                org.apache.flink.util.IOUtils.closeAll(
                    java.util.Arrays.<AutoCloseable>asList(
                        () -> org.apache.arrow.c.StreamFusionArrayAccess.releaseIfOpen(array),
                        schema::release),
                    Throwable.class)) {
      Data.exportVectorSchemaRoot(allocator, input, NativeAllocator.DICTIONARIES, array, schema);
      return encode.apply(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  @Override
  public void close() {
    long owned = handle;
    handle = 0;
    if (owned != 0) close.accept(owned);
  }
}
