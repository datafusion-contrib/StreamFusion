package tech.streamfusion.planner;

/** Describes recoverable linkage failures at optional native admission and diagnostic probes. */
final class NativeProbeFailure {
  private NativeProbeFailure() {}

  static String reason(LinkageError failure) {
    String category;
    if (failure instanceof NoClassDefFoundError) {
      category = "class unavailable";
    } else if (failure instanceof UnsatisfiedLinkError) {
      category = "native library or symbol unavailable";
    } else {
      category = "incompatible method or field signature";
    }
    return "native probe: "
        + category
        + " ("
        + failure.getClass().getSimpleName()
        + "): "
        + failure.getMessage();
  }
}
