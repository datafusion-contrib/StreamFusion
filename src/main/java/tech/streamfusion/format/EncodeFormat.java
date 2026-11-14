package tech.streamfusion.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;

/** A format-owned serializer plan, analogous to Flink's EncodingFormat. */
public final class EncodeFormat implements Serializable {
  private static final long serialVersionUID = 1L;

  public interface OpenCompletion extends Serializable {
    String complete(String options) throws IOException;
  }

  public final int format;
  public final String options;
  public final boolean acceptsChangelog;
  private final OpenCompletion completion;
  private final NativeMessageEncoderFactory factory;

  private EncodeFormat(
      int format,
      String options,
      OpenCompletion completion,
      boolean acceptsChangelog,
      NativeMessageEncoderFactory factory) {
    this.format = format;
    this.options = options;
    this.completion = completion;
    this.acceptsChangelog = acceptsChangelog;
    this.factory = java.util.Objects.requireNonNull(factory);
  }

  public static EncodeFormat of(String identifier, Map<String, String> options, RowType rowType) {
    return plan(identifier, options, rowType).format;
  }

  public static NativeEncoderPlan plan(
      String identifier, Map<String, String> options, RowType rowType) {
    try {
      return NativeFormatProviders.forIdentifier(identifier)
          .map(
              provider ->
                  provider.planEncoder(new NativeFormatContext(rowType, rowType, options, false)))
          .orElseGet(() -> NativeEncoderPlan.fallback("optional format artifact is not installed"));
    } catch (RuntimeException | LinkageError unavailable) {
      return NativeEncoderPlan.fallback(
          "optional native encoder is unavailable: " + unavailable.getClass().getSimpleName());
    }
  }

  public static EncodeFormat resolved(
      int format,
      String options,
      OpenCompletion completion,
      boolean acceptsChangelog,
      NativeMessageEncoderFactory factory) {
    return new EncodeFormat(format, options, completion, acceptsChangelog, factory);
  }

  /** Older job graphs carried format codes/options before factories became format-owned. */
  private Object readResolve() {
    if (factory != null) return this;
    int legacyCode = format;
    NativeMessageEncoderFactory restoredFactory =
        (resolved, types, names, fields) -> {
          String identifier = FormatCodes.identifier(legacyCode);
          var provider =
              NativeFormatProviders.forIdentifier(identifier)
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "The native format artifact is required to restore serializer code "
                                  + legacyCode));
          var restored = provider.encoderFactory();
          if (restored == null)
            throw new IllegalStateException(
                "The installed format has no native encoder: " + identifier);
          return restored.create(resolved, types, names, fields);
        };
    return new EncodeFormat(
        format, options, completion, FormatCodes.isCdc(format), restoredFactory);
  }

  public NativeMessageEncoder createEncoder(String[] types, String[] names, int[] fields)
      throws Exception {
    return factory.create(openOptions(), types, names, fields);
  }

  public String openOptions() throws IOException {
    return completion == null ? options : completion.complete(options);
  }

  /** Compatibility conveniences; option/type admission remains inside the installed provider. */
  public static EncodeFormat json(Map<String, String> options) {
    return of("json", options, null);
  }

  public static EncodeFormat csv(Map<String, String> options) {
    return of("csv", options, null);
  }
}
