package io.github.jordepic.streamfusion.format.json;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatOptions;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;

/** Native providers for Flink's JSON and JSON-CDC formats. */
public final class JsonFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "json";
  }

  @Override
  public boolean honorsProjection() {
    return true;
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
    return () -> new JsonDecoder(0, context.ignoreParseErrors(), NativeFormatOptions.encode(context.options()));
  }

  /** A separate provider class shares this JAR for each Flink JSON CDC identifier. */
  public static class Cdc implements NativeFormatProvider {
    private final String identifier;
    private final int format;

    public Cdc(String identifier, int format) {
      this.identifier = identifier;
      this.format = format;
    }

    @Override
    public String formatIdentifier() {
      return identifier;
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
      return () -> new JsonDecoder(format, context.ignoreParseErrors(), NativeFormatOptions.encode(context.options()));
    }
  }

  private static final class JsonDecoder extends NativeSchemaMessageDecoder {
    private final int format;
    private final boolean skipParseErrors;
    private final String formatOptions;

    private JsonDecoder(int format, boolean skipParseErrors, String formatOptions) {
      this.format = format;
      this.skipParseErrors = skipParseErrors;
      this.formatOptions = formatOptions;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeJsonFormat.createDecoder(
          format, schemaArrayAddress, schemaAddress, skipParseErrors, formatOptions);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeJsonFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeJsonFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeJsonFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
