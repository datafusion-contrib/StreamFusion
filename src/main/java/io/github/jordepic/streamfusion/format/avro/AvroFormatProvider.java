package io.github.jordepic.streamfusion.format.avro;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

/** Native provider for Flink's schema-embedded {@code avro} format. */
public final class AvroFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "avro";
  }

  @Override
  public boolean honorsProjection() {
    return true;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    return !context.ignoreParseErrors();
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    String writerSchema = AvroSchemaConverter.convertToSchema(context.writerType().copy(false)).toString();
    String readerSchema =
        context.writerType().equals(context.outputType())
            ? ""
            : AvroSchemaConverter.convertToSchema(context.outputType().copy(false)).toString();
    return () -> new Decoder(writerSchema, readerSchema);
  }

  private static final class Decoder implements NativeMessageDecoder {
    private final String writerSchema;
    private final String readerSchema;
    private long handle;

    private Decoder(String writerSchema, String readerSchema) {
      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
    }

    @Override
    public void open(BufferAllocator allocator, RowType outputType) {
      handle = NativeAvroFormat.createDecoder(false, writerSchema, readerSchema);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeAvroFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeAvroFormat.driverInitAddress();
    }

    @Override
    public long decoderHandle() {
      return handle;
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeAvroFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
