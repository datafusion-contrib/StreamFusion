package tech.streamfusion.format.json;

import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.NativeSchemaMessageDecoder;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;

/** Native providers for Flink's JSON and JSON-CDC formats. */
public final class JsonFormatProvider implements NativeFormatProvider {

  /**
   * Whether every column (and every nested leaf) is a type the native JSON decode converts with
   * Flink's exact semantics — the set {@code native/src/json.rs}'s appender dispatch implements,
   * parity-pinned by {@code JsonDecodeParityTest}. Anything else stays on Flink at plan time
   * instead of reaching a native decode it would fail: BINARY (its fixed-size Arrow carriage
   * cannot hold the arbitrary-length base64 Flink decodes without enforcing the declared length)
   * and the INTERVAL types (unimplemented). A null row type (an identifier-level query with no
   * schema at hand) passes; the planner gates on the resolved schema separately.
   */
  static boolean decodableColumns(RowType rowType) {
    return rowType == null
        || rowType.getChildren().stream().allMatch(JsonFormatProvider::decodableType);
  }

  private static boolean decodableType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case VARBINARY:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DECIMAL:
        return true;
      case ROW:
      case ARRAY:
        return type.getChildren().stream().allMatch(JsonFormatProvider::decodableType);
      case MAP:
      case MULTISET:
        // Flink's JSON deserializer itself rejects a non-string map key (a MULTISET's element is
        // its key); decoding one natively would accept where Flink fails the job.
        LogicalType key = type.getChildren().get(0);
        if (!key.is(LogicalTypeFamily.CHARACTER_STRING)) {
          return false;
        }
        return type.getChildren().stream().allMatch(JsonFormatProvider::decodableType);
      default:
        return false;
    }
  }

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
    return NativeFormatOptions.encode(context.options()) != null
        && decodableColumns(context.outputType());
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    return () ->
        new JsonDecoder(
            FormatCodes.JSON, context.ignoreParseErrors(), NativeFormatOptions.encode(context.options()));
  }

  /** A separate provider class shares this JAR for each Flink JSON CDC identifier. */
  public static class Cdc implements NativeFormatProvider {
    private final String identifier;
    private final int format;

    public Cdc(String identifier) {
      this.identifier = identifier;
      this.format = FormatCodes.forIdentifier(identifier);
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
      // The CDC dialects share the plain decode's type set: the envelope's images are decoded by
      // the same JSON appenders.
      return NativeFormatOptions.encode(context.options()) != null
          && decodableColumns(context.outputType());
    }

    @Override
    public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
      // Capture the code, not `this`: the factory ships in the job graph and providers are not
      // serializable (reading the field inside the lambda would capture the provider).
      int format = this.format;
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
