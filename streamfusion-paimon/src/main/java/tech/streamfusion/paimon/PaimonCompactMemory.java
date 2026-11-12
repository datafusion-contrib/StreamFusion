package tech.streamfusion.paimon;

import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Prepaid reader/encoder memory with headroom for independently owned Arrow output. */
final class PaimonCompactMemory implements AutoCloseable {
  private static final long MIB = 1024 * 1024;
  private static final int MIN_IO_CHUNK_BYTES = 64 * 1024;
  private static final int MAX_IO_CHUNK_BYTES = 4 * 1024 * 1024;
  private static final ThreadLocal<PaimonCompactMemory> CURRENT = new ThreadLocal<>();
  final long readerBytes;
  final long writerBytes;
  private final long owner;
  private final PaimonCompactMemory previous;
  private long readerReserved;
  private byte[] inputChunk = new byte[0];
  private boolean closed;

  static long writerTarget(FileStoreTable table) {
    boolean parquet = table.coreOptions().fileFormatString().equals("parquet");
    return Long.parseLong(
        table
            .options()
            .getOrDefault(
                parquet ? "parquet.block.size" : "orc.stripe.size",
                Long.toString((parquet ? 128 : 64) * MIB)));
  }

  static long threshold(long available, long writerTarget) {
    // Two writer targets cover encoding buffers; a whole projected group covers a batch
    // overshooting that target. Reader and outgoing batch each need one more group.
    long fixed = add(multiply(writerTarget, 2), 2 * MIB);
    long remaining = Math.max(0, available - Math.min(available, fixed));
    // Invert 3E + B(E): staging is constant below 64 KiB, equals E up to 4 MiB,
    // then stays at 4 MiB. Keep the reported threshold identical to runtime admission.
    if (remaining <= 4L * MIN_IO_CHUNK_BYTES)
      return Math.max(0, remaining - MIN_IO_CHUNK_BYTES) / 3;
    if (remaining <= 4L * MAX_IO_CHUNK_BYTES) return remaining / 4;
    return (remaining - MAX_IO_CHUNK_BYTES) / 3;
  }

  static PaimonCompactMemory acquire(long readerBytes, long writerTarget) {
    long writerBytes = add(add(multiply(writerTarget, 2), readerBytes), MIB);
    long totalBytes = add(add(writerBytes, readerBytes), stagingBytes(readerBytes));
    long owner = TaskOffHeapMemory.registerOwner("connector", "Paimon append compaction");
    if (!TaskOffHeapMemory.tryReserveWithHeadroom(owner, totalBytes, add(readerBytes, MIB))) {
      TaskOffHeapMemory.closeOwner(owner);
      return null;
    }
    return new PaimonCompactMemory(owner, readerBytes, writerBytes);
  }

  private PaimonCompactMemory(long owner, long readerBytes, long writerBytes) {
    this.owner = owner;
    this.readerBytes = readerBytes;
    this.writerBytes = writerBytes;
    previous = CURRENT.get();
    CURRENT.set(this);
  }

  static PaimonCompactMemory current() {
    return CURRENT.get();
  }

  private static long stagingBytes(long readerBytes) {
    return Math.max(MIN_IO_CHUNK_BYTES, Math.min(readerBytes, MAX_IO_CHUNK_BYTES));
  }

  synchronized byte[] inputChunk(int requested) {
    int size = (int) Math.min(requested, stagingBytes(readerBytes));
    if (inputChunk.length < size) inputChunk = new byte[size];
    return inputChunk;
  }

  /** JNI callbacks suballocate the reader's prepaid share without charging it twice. */
  public synchronized boolean tryReserveReader(long bytes) {
    if (closed || bytes < 0 || bytes > readerBytes - readerReserved) return false;
    readerReserved += bytes;
    return true;
  }

  public synchronized void releaseReader(long bytes) {
    readerReserved -= bytes;
    if (closed && readerReserved == 0) TaskOffHeapMemory.closeOwner(owner);
  }

  synchronized long readerReservedBytes() {
    return readerReserved;
  }

  void checkWriter(long bytes) {
    if (bytes > writerBytes)
      throw new NativeMemoryLimitException(
          "Paimon compact encoder retained " + bytes + " bytes, allowance=" + writerBytes);
  }

  private static long add(long left, long right) {
    return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  private static long multiply(long bytes, int count) {
    return bytes > Long.MAX_VALUE / count ? Long.MAX_VALUE : bytes * count;
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    CURRENT.set(previous);
    // Cancelled runtime workers may still own decoder reservations during teardown.
    if (readerReserved == 0) TaskOffHeapMemory.closeOwner(owner);
  }
}
