package tech.streamfusion.format.csv;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.NativeSchemaMessageDecoder;

/** Native provider for Flink's CSV value format. */
public final class CsvFormatProvider implements NativeFormatProvider {

  /**
   * Whether every column is a type the native CSV decode converts with Flink's exact semantics —
   * the scalar family. ARRAY/ROW columns (Jackson's array-element-delimiter layer) and the types
   * outside that set stay on Flink. A null row type (an identifier-level query with no schema at
   * hand) passes; the planner gates on the resolved schema separately.
   */
  static boolean decodableColumns(RowType rowType) {
    return rowType == null
        || rowType.getChildren().stream()
            .allMatch(
                type -> {
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
                    case DATE:
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    case DECIMAL:
                      return true;
                    default:
                      return false;
                  }
                });
  }

  @Override
  public tech.streamfusion.format.NativeMessageEncoderFactory encoderFactory() {
    return (options, types, names, fields) ->
        tech.streamfusion.format.csv.NativeCsvFormat.encoder(
            tech.streamfusion.format.FormatCodes.CSV, options, types, names, fields);
  }

  @Override
  public String formatIdentifier() {
    return "csv";
  }

  @Override
  public boolean composesKafkaMetadata() {
    return true;
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
    return NativeFormatOptions.encode(context.options()) != null
        && decodableColumns(context.outputType());
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    return () ->
        new Decoder(context.ignoreParseErrors(), NativeFormatOptions.encode(context.options()));
  }

  @Override
  public tech.streamfusion.format.NativeEncoderPlan planEncoder(NativeFormatContext context) {
    var type = context.writerType();
    String typeFallback = CsvEncodeGate.fallbackReason(type);
    if (typeFallback != null)
      return tech.streamfusion.format.NativeEncoderPlan.fallback(typeFallback);
    if (type != null
        && tech.streamfusion.format.JdkFloatSpelling.containsFloat(type)
        && !tech.streamfusion.format.JdkFloatSpelling.nativeMatchesJvm(
            NativeCsvFormat::spellFloatingPoint))
      return tech.streamfusion.format.NativeEncoderPlan.fallback(
          "jdk float spelling mismatch (JDK 19+)");
    String options = CsvEncodeOptions.encode(context.options());
    if (options == null)
      return tech.streamfusion.format.NativeEncoderPlan.fallback(
          "unsupported CSV serializer options");
    if (!NativeCsvFormat.encoderSupported(tech.streamfusion.format.FormatCodes.CSV))
      return tech.streamfusion.format.NativeEncoderPlan.fallback(
          "CSV native encoder is unavailable");
    return tech.streamfusion.format.NativeEncoderPlan.supported(
        tech.streamfusion.format.EncodeFormat.resolved(
            tech.streamfusion.format.FormatCodes.CSV,
            options,
            null,
            false,
            (resolved, types, names, fields) ->
                NativeCsvFormat.encoder(
                    tech.streamfusion.format.FormatCodes.CSV, resolved, types, names, fields)));
  }

  @Override
  public tech.streamfusion.format.EncodeFormat encodeFormat(NativeFormatContext context) {
    return planEncoder(context).format;
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final boolean skipParseErrors;
    private final String formatOptions;

    private Decoder(boolean skipParseErrors, String formatOptions) {
      this.skipParseErrors = skipParseErrors;
      this.formatOptions = formatOptions;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeCsvFormat.createDecoder(
          schemaArrayAddress, schemaAddress, skipParseErrors, formatOptions);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeCsvFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public void decodeRecordsInto(
        long slabAddress,
        long slabLength,
        long[] layout,
        int count,
        String topic,
        int partition,
        long outArray,
        long outSchema) {
      NativeCsvFormat.decodeRecordsInto(
          handle, slabAddress, slabLength, layout, count, topic, partition, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeCsvFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeCsvFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
