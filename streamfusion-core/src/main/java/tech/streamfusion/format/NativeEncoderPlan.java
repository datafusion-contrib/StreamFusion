package tech.streamfusion.format;

/** One format's exact planning decision, including the reason a connector must keep Flink. */
public final class NativeEncoderPlan {
  public final EncodeFormat format;
  public final String fallbackReason;

  private NativeEncoderPlan(EncodeFormat format, String reason) {
    this.format = format;
    this.fallbackReason = reason;
  }

  public static NativeEncoderPlan supported(EncodeFormat format) {
    return new NativeEncoderPlan(java.util.Objects.requireNonNull(format), null);
  }

  public static NativeEncoderPlan fallback(String reason) {
    return new NativeEncoderPlan(null, java.util.Objects.requireNonNull(reason));
  }
}
