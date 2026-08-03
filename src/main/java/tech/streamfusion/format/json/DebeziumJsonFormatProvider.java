package tech.streamfusion.format.json;

/** Service-loadable Debezium JSON provider. */
public final class DebeziumJsonFormatProvider extends JsonFormatProvider.Cdc {
  public DebeziumJsonFormatProvider() {
    super("debezium-json");
  }
}
