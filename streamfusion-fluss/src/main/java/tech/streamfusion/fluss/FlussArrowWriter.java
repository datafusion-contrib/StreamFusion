package tech.streamfusion.fluss;

import static org.apache.fluss.config.ConfigOptions.*;
import static org.apache.fluss.record.LogRecordBatchFormat.*;
import static tech.streamfusion.fluss.FlussArrowContext.await;
import static tech.streamfusion.fluss.FlussArrowContext.checkError;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.client.write.DynamicPartitionCreator;
import org.apache.fluss.client.write.StickyBucketAssigner;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.rpc.messages.*;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/**
 * Task-thread encoding/credit owner; producer threads retain only encoded bytes and ordered RPCs.
 */
final class FlussArrowWriter implements AutoCloseable {
  private final FlussArrowContext context;
  private final Map<TableBucket, Integer> sequences = new HashMap<>();
  private long writerId = NO_WRITER_ID;
  private final Map<PhysicalTablePath, StickyBucketAssigner> bucketAssigners = new HashMap<>();
  private DynamicPartitionCreator partitionCreator;
  private FlussArrowStatistics statistics;
  private final int[] bucketColumns;
  private final int[] bucketPrecisions;
  private final boolean coalesce =
      Boolean.parseBoolean(System.getProperty("streamfusion.fluss.coalesce.enabled", "true"));
  long encodeNanos;
  final LongAdder produceRpcNanos = new LongAdder();
  final LongAdder produceRequests = new LongAdder();
  private final Deque<PendingAppend> pendingAppends = new ArrayDeque<>();
  private final Map<TableBucket, CompletableFuture<Void>> bucketTails = new HashMap<>();
  private final Map<TableBucket, AppendGroup> appendGroups = new HashMap<>();
  private final AtomicReference<Throwable> appendFailure = new AtomicReference<>();
  private ExecutorService producers;
  private long queuedBytes;
  private volatile boolean closed;

  FlussArrowWriter(FlussArrowContext context, boolean initializePartitions) throws IOException {
    this.context = context;
    bucketColumns =
        context.table.getBucketKeys().stream()
            .mapToInt(context.table.getRowType().getFieldNames()::indexOf)
            .toArray();
    bucketPrecisions =
        java.util.Arrays.stream(bucketColumns)
            .map(
                i -> {
                  var type = context.table.getRowType().getTypeAt(i);
                  if (type instanceof org.apache.fluss.types.TimestampType) {
                    org.apache.fluss.types.TimestampType t =
                        ((org.apache.fluss.types.TimestampType) type);
                    return t.getPrecision();
                  }
                  if (type instanceof org.apache.fluss.types.LocalZonedTimestampType) {
                    org.apache.fluss.types.LocalZonedTimestampType t =
                        ((org.apache.fluss.types.LocalZonedTimestampType) type);
                    return t.getPrecision();
                  }
                  return 0;
                })
            .toArray();
    if (initializePartitions) {
      context.metadata.updateTableOrPartitionMetadata(context.path, null);
      partitionCreator =
          new DynamicPartitionCreator(
              context.metadata,
              context.connection.getAdmin(),
              context.config.get(CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED),
              failure -> appendFailure.compareAndSet(null, failure));
    }
  }

  private static final class PendingAppend {
    private final CompletableFuture<Void> future;
    private final int bytes;
    private final TaskOffHeapMemory.Reservation reservation;

    private PendingAppend(
        CompletableFuture<Void> future, int bytes, TaskOffHeapMemory.Reservation reservation) {
      this.future = future;
      this.bytes = bytes;
      this.reservation = reservation;
    }

    public CompletableFuture<Void> future() {
      return future;
    }

    public int bytes() {
      return bytes;
    }

    public TaskOffHeapMemory.Reservation reservation() {
      return reservation;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      PendingAppend that = (PendingAppend) other;
      return java.util.Objects.equals(future, that.future)
          && bytes == that.bytes
          && java.util.Objects.equals(reservation, that.reservation);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(future);
      result = 31 * result + bytes;
      result = 31 * result + java.util.Objects.hashCode(reservation);
      return result;
    }

    @Override
    public String toString() {
      return "PendingAppend[future="
          + future
          + ", bytes="
          + bytes
          + ", reservation="
          + reservation
          + "]";
    }
  }

  private static final class AppendGroup {
    // Released Fluss 1.0 retains five batches per writer for retry deduplication.
    private static final int MAX_BATCHES = 5;
    private static final long MAX_COALESCED_BYTES = 1024 * 1024;
    private final List<byte[]> records = new ArrayList<>();
    private int bytes;
    private boolean started;
    private CompletableFuture<Void> future;

    AppendGroup(byte[] first) {
      records.add(first);
      bytes = first.length;
    }

    synchronized boolean add(byte[] next, long maxBytes) {
      if (started || records.size() == MAX_BATCHES || (long) bytes + next.length > maxBytes)
        return false;
      records.add(next);
      bytes += next.length;
      return true;
    }

    synchronized List<byte[]> start() {
      started = true;
      List<byte[]> result = List.copyOf(records);
      records.clear();
      return result;
    }

    synchronized void discard() {
      started = true;
      records.clear();
    }
  }

  public void append(VectorSchemaRoot input) throws IOException {
    await(appendAsync(input));
    reapAppends();
  }

  /** Encodes on the task thread, then overlaps independent buckets and subsequent encoding. */
  public CompletableFuture<Void> appendAsync(VectorSchemaRoot input) throws IOException {
    return appendAsync(input, null);
  }

  public CompletableFuture<Void> appendAsync(VectorSchemaRoot input, String partitionName)
      throws IOException {
    if (closed) throw new IOException("Fluss Arrow client is closed");
    checkAppendFailure();
    reapAppends();
    if (context.table.hasPrimaryKey())
      throw new IOException("Primary-key production is not accelerated");

    if (context.table.isPartitioned() != (partitionName != null))
      throw new IOException("Append batch must identify its table partition");
    if (input.getRowCount() == 0) return CompletableFuture.completedFuture(null);
    PhysicalTablePath physical =
        partitionName == null
            ? PhysicalTablePath.of(context.path)
            : PhysicalTablePath.of(context.path, partitionName);
    Long partitionId = resolvePartition(physical);
    int bucketCount =
        context
            .metadata
            .getCluster()
            .getBucketCountOrElseThrow(
                TableOrPartition.of(context.table.getTableId(), partitionId));
    StickyBucketAssigner assigner =
        bucketAssigners.computeIfAbsent(physical, StickyBucketAssigner::new);
    if (context.table.getBucketKeys().isEmpty())
      return appendSized(input, partitionId, bucketCount, assigner, null);
    long split;
    try (var array = org.apache.arrow.c.ArrowArray.allocateNew(context.allocator);
        var schema = org.apache.arrow.c.ArrowSchema.allocateNew(context.allocator)) {
      org.apache.arrow.c.Data.exportVectorSchemaRoot(
          context.allocator,
          input,
          tech.streamfusion.operator.NativeAllocator.DICTIONARIES,
          array,
          schema);
      split =
          NativeFluss.splitByBucket(
              array.memoryAddress(),
              schema.memoryAddress(),
              bucketColumns,
              bucketPrecisions,
              bucketCount);
    }
    try {
      List<CompletableFuture<Void>> parts = new ArrayList<>();
      while (true) {
        try (var array = org.apache.arrow.c.ArrowArray.allocateNew(context.allocator);
            var schema = org.apache.arrow.c.ArrowSchema.allocateNew(context.allocator)) {
          int bucket =
              NativeFluss.nextBucketSlice(split, array.memoryAddress(), schema.memoryAddress());
          if (bucket < 0) break;
          try (var root =
              org.apache.arrow.c.Data.importVectorSchemaRoot(
                  context.allocator,
                  array,
                  schema,
                  tech.streamfusion.operator.NativeAllocator.DICTIONARIES)) {
            parts.add(appendSized(root, partitionId, bucketCount, assigner, bucket));
          }
        }
      }
      return CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new));
    } finally {
      NativeFluss.closeBucketSplit(split);
    }
  }

  private CompletableFuture<Void> appendSized(
      VectorSchemaRoot input,
      Long partitionId,
      int bucketCount,
      StickyBucketAssigner assigner,
      Integer fixedBucket)
      throws IOException {
    long target =
        Math.min(
            context.config.get(CLIENT_WRITER_BATCH_SIZE).getBytes(),
            Math.min(
                context.config.get(CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes(),
                context.config.get(CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes()));
    long inputBytes = 0;
    for (var vector : input.getFieldVectors()) inputBytes += vector.getBufferSize();
    if (input.getRowCount() > 1 && inputBytes > target) {
      int rowsPerBatch =
          (int) Math.max(1, Math.max(1, target - 1024) * input.getRowCount() / inputBytes);
      List<CompletableFuture<Void>> parts = new ArrayList<>();
      for (int offset = 0; offset < input.getRowCount(); offset += rowsPerBatch) {
        try (var part = input.slice(offset, Math.min(rowsPerBatch, input.getRowCount() - offset))) {
          parts.add(appendPart(part, partitionId, bucketCount, assigner, fixedBucket));
        }
      }
      return CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new));
    }
    return appendPart(input, partitionId, bucketCount, assigner, fixedBucket);
  }

  private Long resolvePartition(PhysicalTablePath physical) throws IOException {
    if (physical.getPartitionName() == null) return null;
    while (true) {
      checkAppendFailure();
      var known = context.metadata.getPartitionId(physical);
      if (known.isPresent()) return known.get();
      try {
        // The SDK checks its cache before locking; share cold lookups across writers.
        synchronized (context.metadata) {
          partitionCreator.checkAndCreatePartitionAsync(physical, context.table);
          context.metadata.checkAndUpdatePartitionMetadata(physical);
          return context.metadata.getPartitionIdOrElseThrow(physical);
        }
      } catch (org.apache.fluss.exception.PartitionNotExistException creating) {
        if (!context.config.get(CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED)) throw creating;
        try {
          Thread.sleep(10);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IOException("Fluss partition creation interrupted", interrupted);
        }
      }
    }
  }

  private CompletableFuture<Void> appendPart(
      VectorSchemaRoot input,
      Long partitionId,
      int bucketCount,
      StickyBucketAssigner assigner,
      Integer fixedBucket)
      throws IOException {
    if (writerId == NO_WRITER_ID)
      writerId =
          await(context.metadata.newRandomTabletServerClient().initWriter(new InitWriterRequest()))
              .getWriterId();
    TableBucket bucket =
        new TableBucket(
            context.table.getTableId(),
            partitionId,
            fixedBucket == null
                ? assigner.assignBucket(context.metadata.getCluster(), bucketCount)
                : fixedBucket);
    int sequence = sequences.getOrDefault(bucket, 0);
    long encodeStart = context.profile ? System.nanoTime() : 0;
    byte[] bytes;
    try (VectorSchemaRoot wire =
        FlussArrowSchema.forAppend(input, context.wireSchema, context.allocator)) {
      var compression = context.table.getTableConfig().getArrowCompressionInfo();
      org.apache.arrow.vector.compression.CompressionUtil.CodecType codecType;
      switch (compression.getCompressionType()) {
        case NONE:
          codecType = org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION;
          break;
        case LZ4_FRAME:
          codecType = org.apache.arrow.vector.compression.CompressionUtil.CodecType.LZ4_FRAME;
          break;
        case ZSTD:
          codecType = org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD;
          break;
        default:
          throw new IncompatibleClassChangeError();
      }
      var codec =
          FlussArrowCompression.INSTANCE.createCodec(codecType, compression.getCompressionLevel());
      byte[] stats = null;
      if (context.table.isStatisticsEnabled()) {
        if (statistics == null)
          statistics =
              new FlussArrowStatistics(
                  context.table.getRowType(), context.table.getStatsIndexMapping());
        stats = statistics.collect(input);
      }
      bytes =
          FlussArrowLogBatch.encode(
              wire, context.table.getSchemaId(), writerId, sequence, codec, stats);
    }
    if (context.profile) encodeNanos += System.nanoTime() - encodeStart;
    long limit = context.config.get(CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes();
    if (bytes.length > limit
        || bytes.length > context.config.get(CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes()) {
      if (input.getRowCount() > 1) {
        int half = input.getRowCount() / 2;
        try (var first = input.slice(0, half);
            var second = input.slice(half, input.getRowCount() - half)) {
          return CompletableFuture.allOf(
              appendPart(first, partitionId, bucketCount, assigner, fixedBucket),
              appendPart(second, partitionId, bucketCount, assigner, fixedBucket));
        }
      }
      throw new IOException(
          "A Fluss Arrow record exceeds the configured writer buffer or request size");
    }
    while (!pendingAppends.isEmpty()
        && (queuedBytes + bytes.length > limit || pendingAppends.size() >= 64)) {
      await(pendingAppends.peekFirst().future());
      reapAppends();
    }
    checkAppendFailure();
    if (producers == null)
      producers =
          Executors.newFixedThreadPool(
              4,
              runnable -> {
                Thread thread = new Thread(runnable, "streamfusion-fluss-producer");
                thread.setDaemon(true);
                return thread;
              });
    TaskOffHeapMemory.Reservation reservation =
        context.allocator == NativeAllocator.SHARED
            ? TaskOffHeapMemory.reserve("connector", "Fluss append queue", bytes.length)
            : null;
    CompletableFuture<Void> future;
    try {
      AppendGroup group = appendGroups.get(bucket);
      if (!coalesce
          || group == null
          || !group.add(
              bytes,
              Math.min(
                  AppendGroup.MAX_COALESCED_BYTES,
                  context.config.get(CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes()))) {
        group = new AppendGroup(bytes);
        AppendGroup scheduled = group;
        CompletableFuture<Void> previous =
            bucketTails.getOrDefault(bucket, CompletableFuture.completedFuture(null));
        group.future =
            previous
                .thenRunAsync(
                    () -> {
                      try {
                        produce(bucket, scheduled.start());
                      } catch (IOException failure) {
                        throw new java.util.concurrent.CompletionException(failure);
                      }
                    },
                    producers)
                .whenComplete((ignored, failure) -> scheduled.discard());
        bucketTails.put(bucket, group.future);
        appendGroups.put(bucket, group);
      }
      future =
          group.future.whenComplete(
              (ignored, failure) -> {
                if (reservation != null) reservation.close();
                if (failure != null) appendFailure.compareAndSet(null, failure);
              });
    } catch (Throwable failure) {
      if (reservation != null) reservation.close();
      throw failure;
    }
    sequences.put(bucket, sequence + 1);
    if (fixedBucket == null)
      assigner.onNewBatch(context.metadata.getCluster(), bucketCount, bucket.getBucket());
    pendingAppends.addLast(new PendingAppend(future, bytes.length, reservation));
    queuedBytes += bytes.length;
    return future;
  }

  public void flush() throws IOException {
    while (!pendingAppends.isEmpty()) {
      await(pendingAppends.peekFirst().future());
      reapAppends();
    }
    checkAppendFailure();
  }

  private void reapAppends() throws IOException {
    while (!pendingAppends.isEmpty() && pendingAppends.peekFirst().future().isDone()) {
      PendingAppend append = pendingAppends.removeFirst();
      queuedBytes -= append.bytes();
      await(append.future());
    }
  }

  private void checkAppendFailure() throws IOException {
    Throwable failure = appendFailure.get();
    if (failure != null) await(CompletableFuture.failedFuture(failure));
  }

  private void produce(TableBucket bucket, List<byte[]> batches) throws IOException {
    if (closed) throw new IOException("Fluss Arrow client is closed");
    var records = Unpooled.wrappedBuffer(batches.toArray(byte[][]::new));
    try {
      var request =
          new ProduceLogRequest()
              .setTableId(context.table.getTableId())
              .setAcks(-1)
              .setTimeoutMs(Math.toIntExact(context.config.get(CLIENT_REQUEST_TIMEOUT).toMillis()));
      var requestBucket =
          request
              .addBucketsReq()
              .setBucketId(bucket.getBucket())
              .setRoutingBucketCount(
                  context
                      .metadata
                      .getCluster()
                      .getBucketCountOrElseThrow(
                          TableOrPartition.of(bucket.getTableId(), bucket.getPartitionId())))
              .setRecords(records);
      if (bucket.getPartitionId() != null) requestBucket.setPartitionId(bucket.getPartitionId());
      long produceStart = context.profile ? System.nanoTime() : 0;
      var response =
          context.retry(
              bucket,
              gateway -> {
                if (context.profile) produceRequests.increment();
                return gateway.produceLog(request);
              },
              reply -> validateAppendResponse(reply, bucket),
              context.config.get(CLIENT_WRITER_RETRIES));
      if (context.profile) produceRpcNanos.add(System.nanoTime() - produceStart);
      if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
        response.getParsedByteBuf().release();
    } finally {
      records.release();
    }
  }

  static void validateAppendResponse(ProduceLogResponse reply, TableBucket bucket)
      throws IOException {
    validateAppendResponse(reply, bucket.getBucket());
    var response = reply.getBucketsRespAt(0);
    if (!java.util.Objects.equals(
        response.hasPartitionId() ? response.getPartitionId() : null, bucket.getPartitionId()))
      throw new IOException("Unexpected Fluss produce partition");
  }

  static void validateAppendResponse(ProduceLogResponse reply, int bucket) throws IOException {
    if (reply.getBucketsRespsCount() != 1 || reply.getBucketsRespAt(0).getBucketId() != bucket)
      throw new IOException("Unexpected Fluss produce response");
    var response = reply.getBucketsRespAt(0);
    // The SDK completes a duplicate sequence as an already acknowledged batch.
    if (response.hasErrorCode()
        && Errors.forCode(response.getErrorCode()) == Errors.DUPLICATE_SEQUENCE_EXCEPTION) return;
    checkError(response);
  }

  void stopProducing() {
    closed = true;
    if (producers != null) producers.shutdownNow();
  }

  @Override
  public void close() {
    stopProducing();
    IOException failure =
        new IOException("Fluss Arrow client closed before append acknowledgement");
    for (PendingAppend pending : pendingAppends) {
      pending.future().completeExceptionally(failure);
      if (pending.reservation() != null) pending.reservation().close();
    }
    pendingAppends.clear();
    bucketTails.clear();
    appendGroups.clear();
    queuedBytes = 0;
  }
}
