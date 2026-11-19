package tech.streamfusion.format;

/** Serializable completion carried by the pre-factory compatibility fixture. */
public final class LegacyEncodeCompletion implements EncodeFormat.OpenCompletion {
  private static final long serialVersionUID = 1L;
  private final String suffix;

  public LegacyEncodeCompletion(String suffix) { this.suffix = suffix; }

  @Override public String complete(String options) { return options + suffix; }
}
