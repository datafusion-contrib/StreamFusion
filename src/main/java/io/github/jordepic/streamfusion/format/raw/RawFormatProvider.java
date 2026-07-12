package io.github.jordepic.streamfusion.format.raw;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;

/** Native provider for Flink's raw value format. */
public final class RawFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "raw";
  }

  @Override
  public boolean honorsProjection() {
    return false;
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
    return Decoder::new;
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeRawFormat.createDecoder(schemaArrayAddress, schemaAddress);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeRawFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeRawFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeRawFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
