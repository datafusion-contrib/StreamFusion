package tech.streamfusion.format;

import org.apache.arrow.vector.VectorSchemaRoot;

/** A task-local encoder owned by one optional format library. */
public interface NativeMessageEncoder extends AutoCloseable {
  EncodedMessages encode(VectorSchemaRoot input) throws Exception;

  @Override
  void close() throws Exception;
}
