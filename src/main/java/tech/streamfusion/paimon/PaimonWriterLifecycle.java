package tech.streamfusion.paimon;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.operation.AbstractFileStoreWrite;

/** Marks externally written files in the released Paimon 1.0/2.0 writer-cleanup lifecycle. */
public final class PaimonWriterLifecycle {
  private static final Access ACCESS = access();

  private record Access(Field writers, Field modified) {}

  private PaimonWriterLifecycle() {}

  private static Access access() {
    try {
      Field writers = AbstractFileStoreWrite.class.getDeclaredField("writers");
      Field modified =
          AbstractFileStoreWrite.WriterContainer.class.getDeclaredField(
              "lastModifiedCommitIdentifier");
      if (writers.getType() != Map.class
          || modified.getType() != long.class
          || !writers.trySetAccessible()
          || !modified.trySetAccessible()) {
        return null;
      }
      return new Access(writers, modified);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError unavailable) {
      return null;
    }
  }

  public static boolean supported() {
    return ACCESS != null;
  }

  static void modified(StoreSinkWrite sink, BinaryRow partition, int bucket, long checkpoint)
      throws IOException {
    if (ACCESS == null || !(sink instanceof StoreSinkWriteImpl delegate)) {
      throw new IOException("Paimon native writer lifecycle is unavailable");
    }
    try {
      var writers = (Map<?, ?>) ACCESS.writers().get(delegate.getWrite().getWrite());
      var buckets = (Map<?, ?>) writers.get(partition);
      Object writer = buckets == null ? null : buckets.get(bucket);
      if (writer == null) {
        throw new IOException("Paimon did not open the notified native bucket");
      }
      ACCESS.modified().setLong(writer, Math.max(ACCESS.modified().getLong(writer), checkpoint));
    } catch (IllegalAccessException failure) {
      throw new IOException("Cannot mark the Paimon native data checkpoint", failure);
    }
  }
}
