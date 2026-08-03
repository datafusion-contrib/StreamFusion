package tech.streamfusion.format.raw;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.NativeSchemaMessageDecoder;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;

/**
 * Native provider for Flink's raw value format: the whole message is the single physical column's
 * value. Admitted natively: CHAR/VARCHAR (UTF-8 charset only), VARBINARY, BOOLEAN, and the
 * fixed-width numerics with either {@code raw.endianness}. Staying on Flink: multi-column schemas
 * and invalid option values (Flink raises its own ValidationException); {@code RAW<T>} columns
 * (their bytes belong to a Java TypeSerializer); fixed-length BINARY (Flink passes any message
 * length through where Arrow's fixed-size binary enforces the declared one); a non-UTF-8
 * {@code raw.charset} (the native decode has no charset machinery); and {@code ignore-parse-errors}
 * (an option Flink's raw factory doesn't define).
 */
public final class RawFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "raw";
  }

  /** The single column's admitted roots — kept in a method body (like the sibling providers') so
   * the class links under a Flink-less loader: the extension-JAR probe instantiates providers over
   * the platform classloader, where a static {@code EnumSet<LogicalTypeRoot>} fails resolution.
   * {@code encode} additionally admits fixed-length BINARY: writing the fixed-size boundary bytes
   * verbatim is lossless, while the decode side cannot enforce the declared length on arbitrary
   * message bytes. */
  private static boolean supportedType(RowType schema, boolean encode) {
    switch (schema.getTypeAt(0).getTypeRoot()) {
      case BINARY:
        return encode;
      case CHAR:
      case VARCHAR:
      case VARBINARY:
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  @Override
  public boolean honorsProjection() {
    return false;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    RowType schema = context.writerType();
    return !context.ignoreParseErrors()
        && schema.getFieldCount() == 1
        && supportedType(schema, false)
        && NativeFormatOptions.encode(context.options()) != null;
  }

  /** The sink side of the same format: the single column's value IS the message. A NULLABLE column
   * falls back — Flink serializes a null field as a null {@code byte[]}, a Kafka tombstone, which
   * the sink's value path does not produce. The sink seam hands prefix-stripped options. */
  @Override
  public EncodeFormat encodeFormat(NativeFormatContext context) {
    RowType schema = context.writerType();
    Map<String, String> options = context.options();
    if (schema.getFieldCount() != 1
        || !supportedType(schema, true)
        || schema.getTypeAt(0).isNullable()) {
      return null;
    }
    String charset = options.get("charset");
    if (charset != null && !NativeFormatOptions.isUtf8(charset)) {
      return null;
    }
    String endianness = options.get("endianness");
    if (endianness == null || "big-endian".equalsIgnoreCase(endianness)) {
      return EncodeFormat.resolved(FormatCodes.RAW, "", null);
    }
    if ("little-endian".equalsIgnoreCase(endianness)) {
      return EncodeFormat.resolved(FormatCodes.RAW, "endianness=little-endian\n", null);
    }
    return null; // Flink raises its own ValidationException for an invalid endianness
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    String formatOptions = NativeFormatOptions.encode(context.options());
    return () -> new Decoder(formatOptions);
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final String formatOptions;

    private Decoder(String formatOptions) {
      this.formatOptions = formatOptions;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeRawFormat.createDecoder(schemaArrayAddress, schemaAddress, formatOptions);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeRawFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeRawFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeRawFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
