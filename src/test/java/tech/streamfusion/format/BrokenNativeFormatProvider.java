package tech.streamfusion.format;

/** ServiceLoader fixture that models an extension whose optional dependency is absent. */
public final class BrokenNativeFormatProvider implements NativeFormatProvider {

  static {
    if (true) {
      throw new NoClassDefFoundError("optional-format-dependency");
    }
  }

  @Override
  public String formatIdentifier() {
    return "broken";
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
    return false;
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    throw new UnsupportedOperationException();
  }
}
