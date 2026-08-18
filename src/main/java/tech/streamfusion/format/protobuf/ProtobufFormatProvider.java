package tech.streamfusion.format.protobuf;

import java.io.Serial;
import java.util.Base64;
import java.util.Map;
import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.NativeSchemaMessageDecoder;
import tech.streamfusion.planner.ProtobufDescriptors;

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
    boolean readDefaults =
        Boolean.parseBoolean(context.options().get("protobuf.read-default-values"));
    return !context.ignoreParseErrors()
        && messageClass != null
        && (!readDefaults || ProtobufDescriptors.isProto3Message(messageClass))
        && ProtobufDescriptors.isSupportedMessage(messageClass);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    String messageClass = context.options().get("protobuf.message-class-name");
    // This method runs while Flink translates the physical plan. Resolve the user-supplied generated
    // class here, while the planner's user-code classloader is authoritative, and put only portable
    // data in the Source that Flink serializes to TaskManagers.
    ProtobufDecoderPlan plan =
        new ProtobufDecoderPlan(
            ProtobufDescriptors.descriptorSet(messageClass),
            ProtobufDescriptors.messageName(messageClass),
            Boolean.parseBoolean(context.options().get("protobuf.read-default-values")));
    return new DecoderFactory(plan);
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
    private final boolean readDefaults;

    private Decoder(byte[] descriptor, String messageName, boolean readDefaults) {
      this.descriptor = descriptor;
      this.messageName = messageName;
      this.readDefaults = readDefaults;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeProtobufFormat.createDecoder(
          descriptor, messageName, readDefaults, schemaArrayAddress, schemaAddress);
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

  /** Serializable job-graph payload produced by the planner and consumed once per task decoder. */
  private static final class ProtobufDecoderPlan implements java.io.Serializable {
    @Serial private static final long serialVersionUID = 1L;

    private final byte[] descriptor;
    private final String messageName;
    private final boolean readDefaults;

    private ProtobufDecoderPlan(byte[] descriptor, String messageName, boolean readDefaults) {
      this.descriptor = descriptor.clone();
      this.messageName = messageName;
      this.readDefaults = readDefaults;
    }

    private Decoder createDecoder() {
      return new Decoder(descriptor, messageName, readDefaults);
    }
  }

  /** Named rather than a lambda so accidental task-side descriptor generation cannot creep back in. */
  private static final class DecoderFactory implements NativeMessageDecoderFactory {
    @Serial private static final long serialVersionUID = 1L;

    private final ProtobufDecoderPlan plan;

    private DecoderFactory(ProtobufDecoderPlan plan) {
      this.plan = plan;
    }

    @Override
    public NativeMessageDecoder create() {
      return plan.createDecoder();
    }
  }
}
