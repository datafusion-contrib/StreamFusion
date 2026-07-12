package io.github.jordepic.streamfusion.format.csv;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatOptions;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;

/** Native provider for Flink's CSV value format. */
public final class CsvFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "csv";
  }

  @Override
  public boolean honorsProjection() {
    return false;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return true;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    return NativeFormatOptions.encode(context.options()) != null;
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    return () -> new Decoder(context.ignoreParseErrors(), NativeFormatOptions.encode(context.options()));
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final boolean skipParseErrors;
    private final String formatOptions;

    private Decoder(boolean skipParseErrors, String formatOptions) {
      this.skipParseErrors = skipParseErrors;
      this.formatOptions = formatOptions;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeCsvFormat.createDecoder(schemaArrayAddress, schemaAddress, skipParseErrors, formatOptions);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeCsvFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeCsvFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeCsvFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
