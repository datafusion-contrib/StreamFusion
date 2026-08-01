package io.github.jordepic.streamfusion.format;

/**
 * A native implementation of one Flink value format. Format artifacts register providers with Java's
 * {@link java.util.ServiceLoader}; connectors use this SPI rather than taking a dependency on every
 * format they may carry.
 *
 * <p>Every method signature on this interface must reference only classes this package owns: the
 * extension-JAR smoke test reflects over the interface with no Flink on the classpath, and
 * reflection resolves every declared signature eagerly (a Flink type here is a
 * {@code NoClassDefFoundError} there, even though method BODIES resolve lazily). Flink types ride
 * inside {@link NativeFormatContext} instead. Pinned by {@code NativeFormatProviderContractTest}.
 */
public interface NativeFormatProvider {

  String formatIdentifier();

  boolean honorsProjection();

  boolean supportsIgnoreParseErrors();

  /** Returns whether this artifact supports the table's exact format options. */
  boolean supports(NativeFormatContext context);

  NativeMessageDecoderFactory createDecoder(NativeFormatContext context);

  /**
   * The sink-side encode format for serializing the context's writer row type under this format
   * instance's prefix-stripped options, or null when this artifact does not natively encode that
   * combination — the planner's fallback gate. Formats without a native serializer keep the
   * default.
   */
  default EncodeFormat encodeFormat(NativeFormatContext context) {
    return null;
  }
}
