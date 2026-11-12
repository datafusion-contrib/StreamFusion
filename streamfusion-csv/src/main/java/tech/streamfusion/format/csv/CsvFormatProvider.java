package tech.streamfusion.format.csv;

import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.NativeSchemaMessageDecoder;
import org.apache.flink.table.types.logical.RowType;

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
  public String formatIdentifier() {
    return "csv";
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
    return () -> new Decoder(context.ignoreParseErrors(), NativeFormatOptions.encode(context.options()));
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
      return NativeCsvFormat.createDecoder(schemaArrayAddress, schemaAddress, skipParseErrors, formatOptions);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeCsvFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
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
