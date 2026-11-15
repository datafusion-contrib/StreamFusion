package tech.streamfusion.format;

import java.io.Serializable;

/** The serializable format plan creates its encoder only on the executing task. */
@FunctionalInterface
public interface NativeMessageEncoderFactory extends Serializable {
  NativeMessageEncoder create(
      String options, String[] logicalTypes, String[] fieldNames, int[] fields) throws Exception;
}
