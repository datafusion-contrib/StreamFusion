package tech.streamfusion.format;

import java.io.Serializable;

/** Serializable factory for a task-local native message decoder. */
@FunctionalInterface
public interface NativeMessageDecoderFactory extends Serializable {

  NativeMessageDecoder create();
}
