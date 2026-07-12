package io.github.jordepic.streamfusion.format.protobuf;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;
import io.github.jordepic.streamfusion.planner.ProtobufDescriptors;

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
    return !context.ignoreParseErrors()
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
