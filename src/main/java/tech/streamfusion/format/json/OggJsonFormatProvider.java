package tech.streamfusion.format.json;

/** Service-loadable Oracle GoldenGate JSON provider. */
public final class OggJsonFormatProvider extends JsonFormatProvider.Cdc {
  public OggJsonFormatProvider() {
    super("ogg-json");
  }
}
