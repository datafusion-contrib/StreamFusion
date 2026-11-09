package tech.streamfusion.paimon;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.StatsCollectingSingleFileWriter;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.utils.LongCounter;

/** Sequence compatibility for the released 1.0 writer's uncounted bundle path. */
public final class PaimonAppendSequence {
  private static final Access ACCESS = access();

  private static final class Access {
    final Method bucket;
    final Field sequence;

    Access(Method bucket, Field sequence) {
      this.bucket = bucket;
      this.sequence = sequence;
    }
  }

  private PaimonAppendSequence() {}

  private static Access access() {
    try {
      // Backports with a different counting contract must retain the stock sink.
      if (RowDataFileWriter.class.getMethod("writeBundle", BundleRecords.class).getDeclaringClass()
          != StatsCollectingSingleFileWriter.class) return null;
      var bucket =
          AbstractFileStoreWrite.class.getDeclaredMethod(
              "getWriterWrapper", BinaryRow.class, int.class);
      var sequence = AppendOnlyWriter.class.getDeclaredField("seqNumCounter");
      if (sequence.getType() != LongCounter.class
          || !bucket.trySetAccessible()
          || !sequence.trySetAccessible()) return null;
      return new Access(bucket, sequence);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError unavailable) {
      return null;
    }
  }

  public static boolean supported() {
    return ACCESS != null;
  }

  static LongCounter counter(AbstractFileStoreWrite<?> write, BinaryRow partition, int bucket)
      throws ReflectiveOperationException {
    if (!supported())
      throw new IllegalStateException("Paimon append sequence contract unavailable");
    var container =
        (AbstractFileStoreWrite.WriterContainer<?>) ACCESS.bucket.invoke(write, partition, bucket);
    if (!(container.writer instanceof AppendOnlyWriter))
      throw new IllegalStateException("Paimon append writer shape changed");
    return (LongCounter) ACCESS.sequence.get(container.writer);
  }

  @FunctionalInterface
  interface Write {
    void run() throws Exception;
  }

  static void write(LongCounter counter, long rows, Write write) throws Exception {
    // RollingFileWriter may close and materialize metadata inside this call. The counter must
    // already include this bundle then; a failed call aborts its files and restores the counter.
    counter.add(rows);
    try {
      write.run();
    } catch (Throwable failure) {
      counter.add(-rows);
      throw failure;
    }
  }
}
