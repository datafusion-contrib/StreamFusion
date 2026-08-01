package io.github.jordepic.streamfusion.format.protobuf;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.format.FormatCodes;
import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;
import io.github.jordepic.streamfusion.planner.ProtobufDescriptors;
import java.util.Base64;
import java.util.Map;

/** Native provider for Flink's protobuf value format. */
public final class ProtobufFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "protobuf";
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
    String messageClass = context.options().get("protobuf.message-class-name");
    // read-default-values=true makes Flink materialize default instances for unset message /
    // repeated / map fields where the native decode (and Flink's own default mode) yields NULL.
    return !context.ignoreParseErrors()
        && !Boolean.parseBoolean(context.options().get("protobuf.read-default-values"))
        && messageClass != null
        && ProtobufDescriptors.isSupportedMessage(messageClass);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    String messageClass = context.options().get("protobuf.message-class-name");
    return () ->
        new Decoder(
            ProtobufDescriptors.descriptorSet(messageClass), ProtobufDescriptors.messageName(messageClass));
  }

  /** The sink seam hands prefix-stripped options; {@code read-default-values} is decode-only (the
   * serializer never consults it), so only the row↔descriptor mapping and the null-string literal
   * gate the encode. */
  @Override
  public EncodeFormat encodeFormat(NativeFormatContext context) {
    Map<String, String> options = context.options();
    String messageClass = options.get("message-class-name");
    String nullLiteral = options.getOrDefault("write-null-string-literal", "");
    if (messageClass == null
        || nullLiteral.contains("\n")
        || nullLiteral.contains("\r")
        || !ProtobufDescriptors.encodes(messageClass, context.writerType())) {
      return null;
    }
    StringBuilder encoded = new StringBuilder();
    encoded
        .append("descriptor=")
        .append(Base64.getEncoder().encodeToString(ProtobufDescriptors.descriptorSet(messageClass)))
        .append('\n')
        .append("message=")
        .append(ProtobufDescriptors.messageName(messageClass))
        .append('\n');
    if (!nullLiteral.isEmpty()) {
      encoded.append("null-literal=").append(nullLiteral).append('\n');
    }
    return EncodeFormat.resolved(FormatCodes.PROTOBUF, encoded.toString(), null);
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final byte[] descriptor;
    private final String messageName;

    private Decoder(byte[] descriptor, String messageName) {
      this.descriptor = descriptor;
      this.messageName = messageName;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeProtobufFormat.createDecoder(descriptor, messageName, schemaArrayAddress, schemaAddress);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeProtobufFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeProtobufFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeProtobufFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
