package tech.streamfusion.fluss;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.rpc.messages.ProduceLogResponse;

/** Public columnar client; independent read/write owners share one explicit transport lease. */
public final class FlussArrowClient implements AutoCloseable {
  public static boolean supportedTransport() {
    return !Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.direct-receive.enabled", "true"))
        || !Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.frame-aware-receive.enabled", "true"))
        || FlussDirectReceive.supported();
  }

  public static final class Fetched implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final long nextOffset;

    public Fetched(VectorSchemaRoot root, long nextOffset) {
      this.root = root;
      this.nextOffset = nextOffset;
    }

    public VectorSchemaRoot root() {
      return root;
    }

    public long nextOffset() {
      return nextOffset;
    }

    @Override
    public void close() {
      if (root != null) root.close();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Fetched that = (Fetched) other;
      return java.util.Objects.equals(root, that.root) && nextOffset == that.nextOffset;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(root);
      result = 31 * result + Long.hashCode(nextOffset);
      return result;
    }

    @Override
    public String toString() {
      return "Fetched[root=" + root + ", nextOffset=" + nextOffset + "]";
    }
  }

  public static final class TransportProfile {
    private final long fetchRpcNanos;
    private final long decodeNanos;
    private final long encodeNanos;
    private final long produceRpcNanos;
    private final long produceRequests;
    private final long receivedRecordBytes;
    private final long borrowedRecordBytes;

    public TransportProfile(
        long fetchRpcNanos,
        long decodeNanos,
        long encodeNanos,
        long produceRpcNanos,
        long produceRequests,
        long receivedRecordBytes,
        long borrowedRecordBytes) {
      this.fetchRpcNanos = fetchRpcNanos;
      this.decodeNanos = decodeNanos;
      this.encodeNanos = encodeNanos;
      this.produceRpcNanos = produceRpcNanos;
      this.produceRequests = produceRequests;
      this.receivedRecordBytes = receivedRecordBytes;
      this.borrowedRecordBytes = borrowedRecordBytes;
    }

    public long fetchRpcNanos() {
      return fetchRpcNanos;
    }

    public long decodeNanos() {
      return decodeNanos;
    }

    public long encodeNanos() {
      return encodeNanos;
    }

    public long produceRpcNanos() {
      return produceRpcNanos;
    }

    public long produceRequests() {
      return produceRequests;
    }

    public long receivedRecordBytes() {
      return receivedRecordBytes;
    }

    public long borrowedRecordBytes() {
      return borrowedRecordBytes;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      TransportProfile that = (TransportProfile) other;
      return fetchRpcNanos == that.fetchRpcNanos
          && decodeNanos == that.decodeNanos
          && encodeNanos == that.encodeNanos
          && produceRpcNanos == that.produceRpcNanos
          && produceRequests == that.produceRequests
          && receivedRecordBytes == that.receivedRecordBytes
          && borrowedRecordBytes == that.borrowedRecordBytes;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + Long.hashCode(fetchRpcNanos);
      result = 31 * result + Long.hashCode(decodeNanos);
      result = 31 * result + Long.hashCode(encodeNanos);
      result = 31 * result + Long.hashCode(produceRpcNanos);
      result = 31 * result + Long.hashCode(produceRequests);
      result = 31 * result + Long.hashCode(receivedRecordBytes);
      result = 31 * result + Long.hashCode(borrowedRecordBytes);
      return result;
    }

    @Override
    public String toString() {
      return "TransportProfile[fetchRpcNanos="
          + fetchRpcNanos
          + ", decodeNanos="
          + decodeNanos
          + ", encodeNanos="
          + encodeNanos
          + ", produceRpcNanos="
          + produceRpcNanos
          + ", produceRequests="
          + produceRequests
          + ", receivedRecordBytes="
          + receivedRecordBytes
          + ", borrowedRecordBytes="
          + borrowedRecordBytes
          + "]";
    }
  }

  private final FlussArrowContext context;
  private final FlussArrowReader reader;
  private final FlussArrowWriter writer;

  public TransportProfile transportProfile() {
    return new TransportProfile(
        reader.fetchRpcNanos,
        reader.decodeNanos,
        writer.encodeNanos,
        writer.produceRpcNanos.sum(),
        writer.produceRequests.sum(),
        reader.receivedRecordBytes,
        reader.borrowedRecordBytes);
  }

  public FlussArrowClient(
      Configuration config, TablePath path, Schema outputSchema, BufferAllocator allocator)
      throws IOException {
    this(config, path, outputSchema, allocator, true);
  }

  public FlussArrowClient(
      Configuration config,
      TablePath path,
      Schema outputSchema,
      BufferAllocator allocator,
      boolean projectRead)
      throws IOException {
    this(config, path, outputSchema, allocator, projectRead, null);
  }

  public FlussArrowClient(
      Configuration config,
      TablePath path,
      Schema outputSchema,
      BufferAllocator allocator,
      boolean projectRead,
      Predicate recordBatchFilter)
      throws IOException {
    this(
        FlussArrowContext.open(config, path, allocator),
        outputSchema,
        projectRead,
        recordBatchFilter);
  }

  FlussArrowClient(
      FlussArrowContext context,
      Schema outputSchema,
      boolean projectRead,
      Predicate recordBatchFilter)
      throws IOException {
    this.context = context;
    FlussArrowReader openedReader = null;
    try {
      openedReader = new FlussArrowReader(context, outputSchema, projectRead, recordBatchFilter);
      writer = new FlussArrowWriter(context, !projectRead);
      reader = openedReader;
    } catch (Throwable failure) {
      try {
        tech.streamfusion.operator.NativeResources.closeAll(openedReader, context);
      } catch (Exception close) {
        failure.addSuppressed(close);
      }
      throw failure;
    }
  }

  public TableInfo tableInfo() {
    return context.table;
  }

  public List<Fetched> fetch(LogSplit split) throws IOException {
    return reader.fetch(split);
  }

  public void append(VectorSchemaRoot input) throws IOException {
    writer.append(input);
  }

  public CompletableFuture<Void> appendAsync(VectorSchemaRoot input) throws IOException {
    return writer.appendAsync(input);
  }

  public CompletableFuture<Void> appendAsync(VectorSchemaRoot input, String partitionName)
      throws IOException {
    return writer.appendAsync(input, partitionName);
  }

  public void flush() throws IOException {
    writer.flush();
  }

  static void validateAppendResponse(ProduceLogResponse reply, TableBucket bucket)
      throws IOException {
    FlussArrowWriter.validateAppendResponse(reply, bucket);
  }

  static void validateAppendResponse(ProduceLogResponse reply, int bucket) throws IOException {
    FlussArrowWriter.validateAppendResponse(reply, bucket);
  }

  static <T> T await(CompletableFuture<T> future) throws IOException {
    return FlussArrowContext.await(future);
  }

  @Override
  public void close() throws Exception {
    writer.stopProducing();
    try {
      reader.close();
    } finally {
      try {
        context.close();
      } finally {
        // Transport teardown precedes pending completion, preserving in-flight request ownership.
        writer.close();
      }
    }
  }
}
