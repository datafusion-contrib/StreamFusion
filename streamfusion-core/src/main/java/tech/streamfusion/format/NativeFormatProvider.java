package tech.streamfusion.format;

/**
 * A native implementation of one Flink value format. Format artifacts register providers with
 * Java's {@link java.util.ServiceLoader}; connectors use this SPI rather than taking a dependency
 * on every format they may carry.
 *
 * <p>Every method signature on this interface must reference only classes this package owns: the
 * extension-JAR smoke test reflects over the interface with no Flink on the classpath, and
 * reflection resolves every declared signature eagerly (a Flink type here is a {@code
 * NoClassDefFoundError} there, even though method BODIES resolve lazily). Flink types ride inside
 * {@link NativeFormatContext} instead. Pinned by {@code NativeFormatProviderContractTest}.
 */
public interface NativeFormatProvider {

  String formatIdentifier();

  boolean honorsProjection();

  boolean supportsIgnoreParseErrors();

  /** Returns whether this artifact supports the table's exact format options. */
  boolean supports(NativeFormatContext context);

  NativeMessageDecoderFactory createDecoder(NativeFormatContext context);

  /** Runtime encoder entry point owned by this provider, independent of its planning inputs. */
  default NativeMessageEncoderFactory encoderFactory() {
    return null;
  }

  /** Exact sink admission for this format's schema and prefix-stripped options. */
  default NativeEncoderPlan planEncoder(NativeFormatContext context) {
    EncodeFormat format = encodeFormat(context);
    return format == null
        ? NativeEncoderPlan.fallback("not natively encoded with these serializer options or types")
        : NativeEncoderPlan.supported(format);
  }

  /** The admitted runtime plan, or null for formats without a native serializer. */
  default EncodeFormat encodeFormat(NativeFormatContext context) {
    return null;
  }
}
