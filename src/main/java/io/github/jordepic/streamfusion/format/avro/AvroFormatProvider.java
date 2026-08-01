package io.github.jordepic.streamfusion.format.avro;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.format.FormatCodes;
import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatOptions;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;
import java.util.Map;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

/** Native provider for Flink's schema-embedded {@code avro} format. */
public final class AvroFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "avro";
  }

  @Override
  public boolean honorsProjection() {
    return true;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    if (context.ignoreParseErrors()) {
      return false;
    }
    String encoding = NativeFormatOptions.option(context.options(), "encoding");
    if (encoding != null && !"binary".equalsIgnoreCase(encoding)) {
      // Avro's JSON encoding is a different wire format the native decode doesn't read.
      return false;
    }
    return AvroDecodeGate.supports(context.writerType(), legacyTimestampMapping(context.options()));
  }

  /** Flink's {@code avro.timestamp_mapping.legacy}, default true. */
  private static boolean legacyTimestampMapping(Map<String, String> options) {
    return !"false".equalsIgnoreCase(NativeFormatOptions.option(options, "timestamp_mapping.legacy"));
  }

  @Override
  public EncodeFormat encodeFormat(NativeFormatContext context) {
    // The sink seam hands each format instance its prefix-stripped options (a key format has no
    // value-format spelling for NativeFormatOptions to resolve).
    RowType rowType = context.writerType();
    Map<String, String> options = context.options();
    if (!"binary".equalsIgnoreCase(options.getOrDefault("encoding", "binary"))) {
      return null;
    }
    boolean legacy = !"false".equalsIgnoreCase(options.get("timestamp_mapping.legacy"));
    if (!AvroEncodeGate.supports(rowType, legacy)) {
      return null;
    }
    // The derived writer schema travels to the native encoder verbatim, so the wire bytes carry
    // Flink's exact record names, union order, and logical types.
    String schema = AvroEncodeGate.derivedSchema(rowType, legacy);
    return EncodeFormat.resolved(FormatCodes.AVRO, "avro-schema=" + schema + "\n", null);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    boolean legacy = legacyTimestampMapping(context.options());
    String writerSchema =
        AvroSchemaConverter.convertToSchema(context.writerType().copy(false), legacy).toString();
    String readerSchema =
        context.writerType().equals(context.outputType())
            ? ""
            : AvroSchemaConverter.convertToSchema(context.outputType().copy(false), legacy).toString();
    return () -> new Decoder(writerSchema, readerSchema);
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final String writerSchema;
    private final String readerSchema;

    private Decoder(String writerSchema, String readerSchema) {
      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeAvroFormat.createDecoder(
          false, writerSchema, readerSchema, schemaArrayAddress, schemaAddress);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeAvroFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeAvroFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeAvroFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
