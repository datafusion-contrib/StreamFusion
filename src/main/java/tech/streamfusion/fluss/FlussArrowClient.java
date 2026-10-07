package tech.streamfusion.fluss;

import static org.apache.fluss.config.ConfigOptions.*;
import static org.apache.fluss.record.LogRecordBatchFormat.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.log.RemoteLogDownloader;
import org.apache.fluss.client.write.StickyBucketAssigner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.*;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.util.CommonRpcMessageUtils;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Java connection, metadata routing and RPC transport for columnar Fluss log traffic. */
public final class FlussArrowClient implements AutoCloseable {
  public static boolean supportedTransport() {
    return !Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.direct-receive.enabled", "true"))
        || !Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.frame-aware-receive.enabled", "true"))
        || FlussDirectReceive.supported();
  }

  public record Fetched(VectorSchemaRoot root, long nextOffset) implements AutoCloseable {
    @Override
    public void close() {
      if (root != null) root.close();
    }
  }

  private final FlussConnections.Lease connectionLease;
  private final FlussConnection connection;
  private final MetadataUpdater metadata;
  private final TablePath path;
  private final TableInfo table;
  private final Configuration config;
  private final BufferAllocator allocator;
  private final Schema outputSchema;
  private final int[] projectedFields;
  private final Map<Integer, Schema> schemas = new HashMap<>();
  private final Map<Integer, Integer> sequences = new HashMap<>();
  private RemoteLogDownloader remoteDownloader;
  private long writerId = NO_WRITER_ID;
  private final StickyBucketAssigner bucketAssigner;
  private final boolean profile = Boolean.getBoolean("streamfusion.fluss.profile");
  private final boolean coalesce =
      Boolean.parseBoolean(System.getProperty("streamfusion.fluss.coalesce.enabled", "true"));
  private long fetchRpcNanos;
  private long decodeNanos;
  private long encodeNanos;
  private long receivedRecordBytes;
  private long borrowedRecordBytes;
  private final LongAdder produceRpcNanos = new LongAdder();
  private final LongAdder produceRequests = new LongAdder();
  private final Deque<PendingAppend> pendingAppends = new ArrayDeque<>();
  private final Map<Integer, CompletableFuture<Void>> bucketTails = new HashMap<>();
  private final Map<Integer, AppendGroup> appendGroups = new HashMap<>();
  private final AtomicReference<Throwable> appendFailure = new AtomicReference<>();
  private ExecutorService producers;
  private long queuedBytes;
  private volatile boolean closed;

  private record PendingAppend(
      CompletableFuture<Void> future, int bytes, TaskOffHeapMemory.Reservation reservation) {}

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

  public record TransportProfile(
      long fetchRpcNanos,
      long decodeNanos,
      long encodeNanos,
      long produceRpcNanos,
      long produceRequests,
      long receivedRecordBytes,
      long borrowedRecordBytes) {}

  public TransportProfile transportProfile() {
    return new TransportProfile(
        fetchRpcNanos,
        decodeNanos,
        encodeNanos,
        produceRpcNanos.sum(),
        produceRequests.sum(),
        receivedRecordBytes,
        borrowedRecordBytes);
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
    this.config = config;
    this.path = path;
    this.outputSchema = outputSchema;
    this.allocator = allocator;
    bucketAssigner = new StickyBucketAssigner(PhysicalTablePath.of(path));
    connectionLease = FlussConnections.acquire(config);
    connection = connectionLease.connection();
    metadata = connection.getMetadataUpdater();
    try {
      table = await(connection.getAdmin().getTableInfo(path));
      if (!projectRead) metadata.updateTableOrPartitionMetadata(path, null);
      schemas.put(table.getSchemaId(), FlussArrowSchema.wireSchema(table.getRowType()));
      // Released 1.0 schemas start at 1 and evolve only by appending nullable columns.
      // Broker projection is positional, so old batches cannot supply newly added positions.
      boolean prune = projectRead && table.getSchemaId() == 1;
      projectedFields =
          java.util.stream.IntStream.range(0, table.getRowType().getFieldCount())
              .filter(
                  i ->
                      !prune
                          || outputSchema.getFields().stream()
                              .anyMatch(
                                  field ->
                                      field
                                          .getName()
                                          .equals(table.getRowType().getFields().get(i).getName())))
              .toArray();
      if (projectedFields.length == 0)
        throw new IOException("Empty Fluss projection is not accelerated");
    } catch (Throwable failure) {
      try {
        connectionLease.close();
      } catch (Exception close) {
        failure.addSuppressed(close);
      }
      throw failure;
    }
  }

  public TableInfo tableInfo() {
    return table;
  }

  public List<Fetched> fetch(LogSplit split) throws IOException {
    TableBucket bucket = split.getTableBucket();
    FetchLogRequest request =
        new FetchLogRequest()
            .setFollowerServerId(-1)
            .setReadPreference(config.get(CLIENT_SCANNER_LOG_READ_PREFERENCE).value())
            .setMaxBytes(Math.toIntExact(config.get(CLIENT_SCANNER_LOG_FETCH_MAX_BYTES).getBytes()))
            .setMinBytes(Math.toIntExact(config.get(CLIENT_SCANNER_LOG_FETCH_MIN_BYTES).getBytes()))
            .setMaxWaitMs(
                Math.toIntExact(config.get(CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME).toMillis()));
    var requestTable = request.addTablesReq().setTableId(bucket.getTableId());
    boolean projected = projectedFields.length < table.getRowType().getFieldCount();
    requestTable.setProjectionPushdownEnabled(projected);
    if (projected) requestTable.setProjectedFields(projectedFields);
    var requestBucket =
        requestTable
            .addBucketsReq()
            .setBucketId(bucket.getBucket())
            .setFetchOffset(split.getStartingOffset())
            .setMaxFetchBytes(
                Math.toIntExact(
                    config.get(CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET).getBytes()));
    if (bucket.getPartitionId() != null) requestBucket.setPartitionId(bucket.getPartitionId());
    long rpcStart = profile ? System.nanoTime() : 0;
    FetchLogResponse response =
        retry(
            bucket,
            gateway -> gateway.fetchLog(request),
            reply -> {
              if (reply.getTablesRespsCount() != 1
                  || reply.getTablesRespAt(0).getBucketsRespsCount() != 1) {
                throw new IOException("Unexpected Fluss fetch reply cardinality");
              }
              var tableResponse = reply.getTablesRespAt(0);
              var bucketResponse = tableResponse.getBucketsRespAt(0);
              if (tableResponse.getTableId() != bucket.getTableId()
                  || bucketResponse.getBucketId() != bucket.getBucket()) {
                throw new IOException("Unexpected Fluss fetch reply identity");
              }
              checkError(bucketResponse);
            },
            Integer.MAX_VALUE);
    if (profile) fetchRpcNanos += System.nanoTime() - rpcStart;
    long decodeStart = profile ? System.nanoTime() : 0;
    List<Fetched> result = new ArrayList<>();
    try {
      var bucketResponse = response.getTablesRespAt(0).getBucketsRespAt(0);
      var fetch = CommonRpcMessageUtils.getFetchLogResultForBucket(bucket, path, bucketResponse);
      long stop =
          Math.min(split.getStoppingOffset().orElse(Long.MAX_VALUE), fetch.getHighWatermark());
      if (fetch.fetchFromRemote()) {
        fetchRemote(fetch.remoteLogFetchInfo(), split.getStartingOffset(), stop, result);
      } else if (bucketResponse.hasRecords()) {
        ByteBuffer records = CommonRpcMessageUtils.toByteBuffer(bucketResponse.getRecordsSlice());
        try (ArrowBuf received =
            FlussReceiveBuffer.borrow(response.getParsedByteBuf(), allocator)) {
          var slice = bucketResponse.getRecordsSlice();
          ArrowBuf borrowedRecords =
              received != null && slice.hasMemoryAddress()
                  ? received.slice(
                      slice.memoryAddress() + slice.readerIndex() - received.memoryAddress(),
                      slice.readableBytes())
                  : null;
          if (profile) {
            receivedRecordBytes += records.remaining();
            if (borrowedRecords != null) borrowedRecordBytes += records.remaining();
          }
          decodeRecords(
              records, split.getStartingOffset(), stop, projected, true, borrowedRecords, result);
        }
      }
      if (fetch.hasFilteredEndOffset())
        result.add(new Fetched(null, Math.min(fetch.getFilteredEndOffset(), stop)));
      return result;
    } catch (Throwable failure) {
      result.forEach(Fetched::close);
      throw failure;
    } finally {
      if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
        response.getParsedByteBuf().release();
      if (profile) decodeNanos += System.nanoTime() - decodeStart;
    }
  }

  private void fetchRemote(RemoteLogFetchInfo info, long start, long stop, List<Fetched> result)
      throws IOException {
    if (remoteDownloader == null) {
      remoteDownloader =
          new RemoteLogDownloader(
              "streamfusion",
              config,
              connection.getOrCreateRemoteFileDownloader(),
              new ScannerMetricGroup(connection.getClientMetricGroup(), path));
      remoteDownloader.start();
    }
    boolean first = true;
    for (var segment : info.remoteLogSegmentList()) {
      var future =
          remoteDownloader.requestRemoteLog(new FsPath(info.remoteLogTabletDir()), segment);
      try (FileLogRecords records = future.getFileLogRecords(first ? info.firstStartPos() : 0)) {
        int position = 0;
        while (position < records.sizeInBytes()) {
          ByteBuffer header =
              ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC).order(ByteOrder.LITTLE_ENDIAN);
          records.readInto(header, position);
          if (header.remaining() != HEADER_SIZE_UP_TO_MAGIC)
            throw new IOException("Truncated remote log header");
          long size = (long) header.getInt(LENGTH_OFFSET) + LOG_OVERHEAD;
          if (size < HEADER_SIZE_UP_TO_MAGIC || size > records.sizeInBytes() - position)
            throw new IOException("Invalid remote log batch length");
          ByteBuffer batch = ByteBuffer.allocate(Math.toIntExact(size));
          records.readInto(batch, position);
          decodeRecords(batch, start, stop, false, false, null, result);
          position += Math.toIntExact(size);
        }
      } finally {
        future.getRecycleCallback().run();
      }
      first = false;
    }
  }

  private void decodeRecords(
      ByteBuffer bytes,
      long start,
      long stop,
      boolean projected,
      boolean allowIncompleteTail,
      ArrowBuf borrowedRecords,
      List<Fetched> output)
      throws IOException {
    ByteBuffer records = bytes.slice().order(ByteOrder.LITTLE_ENDIAN);
    while (records.hasRemaining()) {
      if (records.remaining() < HEADER_SIZE_UP_TO_MAGIC) {
        if (allowIncompleteTail) break;
        throw new IOException("Truncated fetch log header");
      }
      int position = records.position();
      long size = (long) records.getInt(position + LENGTH_OFFSET) + LOG_OVERHEAD;
      if (size < HEADER_SIZE_UP_TO_MAGIC) throw new IOException("Invalid fetch log batch length");
      // Fetch limits may cut the final batch; resume from the last complete batch offset.
      if (size > records.remaining()) {
        if (allowIncompleteTail) break;
        throw new IOException("Truncated fetch log batch");
      }
      ByteBuffer batch = records.slice().order(ByteOrder.LITTLE_ENDIAN);
      batch.limit(Math.toIntExact(size));
      byte magic = batch.get(MAGIC_OFFSET);
      if (magic < LOG_MAGIC_VALUE_V0
          || magic > LOG_MAGIC_VALUE_V2
          || batch.remaining() < recordBatchHeaderSize(magic)) {
        throw new IOException("Invalid fetch log header");
      }
      int schemaId = Short.toUnsignedInt(batch.getShort(schemaIdOffset(magic)));
      Schema schema = schema(schemaId);
      if (projected) {
        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        for (int index : projectedFields) {
          if (index >= schema.getFields().size())
            throw new IOException("Fluss projection exceeds historical schema");
          fields.add(schema.getFields().get(index));
        }
        schema = new Schema(fields);
      }
      try (var decoded =
          FlussArrowLogBatch.decode(
              batch,
              schema,
              allocator,
              !projected,
              borrowedRecords == null ? null : borrowedRecords.slice(position, size))) {
        long next = Math.min(stop, decoded.nextOffset());
        if (next > start && decoded.baseOffset() < stop) {
          if (decoded.root() == null) {
            output.add(new Fetched(null, next));
          } else {
            int from = Math.toIntExact(Math.max(0L, start - decoded.baseOffset()));
            int count = Math.toIntExact(next - decoded.baseOffset()) - from;
            if (count > 0) {
              if (from == 0 && count == decoded.root().getRowCount()) {
                output.add(
                    new Fetched(
                        FlussArrowSchema.convert(decoded.root(), outputSchema, allocator), next));
              } else {
                try (VectorSchemaRoot view = decoded.root().slice(from, count)) {
                  output.add(
                      new Fetched(FlussArrowSchema.convert(view, outputSchema, allocator), next));
                }
              }
            }
          }
        }
      }
      records.position(position + Math.toIntExact(size));
    }
  }

  private Schema schema(int id) throws IOException {
    Schema schema = schemas.get(id);
    if (schema == null) {
      var info = await(connection.getAdmin().getTableSchema(path, id));
      schema = FlussArrowSchema.wireSchema(info.getSchema().getRowType());
      schemas.put(id, schema);
    }
    return schema;
  }

  public void append(VectorSchemaRoot input) throws IOException {
    await(appendAsync(input));
    reapAppends();
  }

  /** Encodes on the task thread, then overlaps independent buckets and subsequent encoding. */
  public CompletableFuture<Void> appendAsync(VectorSchemaRoot input) throws IOException {
    if (closed) throw new IOException("Fluss Arrow client is closed");
    checkAppendFailure();
    reapAppends();
    if (table.hasPrimaryKey()) throw new IOException("Primary-key production is not accelerated");
    if (table.isStatisticsEnabled())
      throw new IOException("Fluss batch statistics require the stock append writer");
    if (!table.getBucketKeys().isEmpty() || !table.getPartitionKeys().isEmpty())
      throw new IOException("Append routing requires a non-partitioned table without bucket keys");
    if (input.getRowCount() == 0) return CompletableFuture.completedFuture(null);
    long target =
        Math.min(
            config.get(CLIENT_WRITER_BATCH_SIZE).getBytes(),
            Math.min(
                config.get(CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes(),
                config.get(CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes()));
    long inputBytes = 0;
    for (var vector : input.getFieldVectors()) inputBytes += vector.getBufferSize();
    if (input.getRowCount() > 1 && inputBytes > target) {
      int rowsPerBatch =
          (int) Math.max(1, Math.max(1, target - 1024) * input.getRowCount() / inputBytes);
      List<CompletableFuture<Void>> parts = new ArrayList<>();
      for (int offset = 0; offset < input.getRowCount(); offset += rowsPerBatch) {
        try (var part = input.slice(offset, Math.min(rowsPerBatch, input.getRowCount() - offset))) {
          parts.add(appendPart(part));
        }
      }
      return CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new));
    }
    return appendPart(input);
  }

  private CompletableFuture<Void> appendPart(VectorSchemaRoot input) throws IOException {
    if (writerId == NO_WRITER_ID)
      writerId =
          await(metadata.newRandomTabletServerClient().initWriter(new InitWriterRequest()))
              .getWriterId();
    int bucket = bucketAssigner.assignBucket(metadata.getCluster(), table.getNumBuckets());
    int sequence = sequences.getOrDefault(bucket, 0);
    long encodeStart = profile ? System.nanoTime() : 0;
    byte[] bytes;
    try (VectorSchemaRoot wire =
        FlussArrowSchema.forAppend(input, schemas.get(table.getSchemaId()), allocator)) {
      var compression = table.getTableConfig().getArrowCompressionInfo();
      var codecType =
          switch (compression.getCompressionType()) {
            case NONE ->
                org.apache.arrow.vector.compression.CompressionUtil.CodecType.NO_COMPRESSION;
            case LZ4_FRAME ->
                org.apache.arrow.vector.compression.CompressionUtil.CodecType.LZ4_FRAME;
            case ZSTD -> org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD;
          };
      var codec =
          FlussArrowCompression.INSTANCE.createCodec(codecType, compression.getCompressionLevel());
      bytes = FlussArrowLogBatch.encode(wire, table.getSchemaId(), writerId, sequence, codec);
    }
    if (profile) encodeNanos += System.nanoTime() - encodeStart;
    long limit = config.get(CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes();
    if (bytes.length > limit
        || bytes.length > config.get(CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes()) {
      if (input.getRowCount() > 1) {
        int half = input.getRowCount() / 2;
        try (var first = input.slice(0, half);
            var second = input.slice(half, input.getRowCount() - half)) {
          return CompletableFuture.allOf(appendPart(first), appendPart(second));
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
              Math.min(4, table.getNumBuckets()),
              runnable -> {
                Thread thread = new Thread(runnable, "streamfusion-fluss-producer");
                thread.setDaemon(true);
                return thread;
              });
    TaskOffHeapMemory.Reservation reservation =
        allocator == NativeAllocator.SHARED
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
                  config.get(CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes()))) {
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
    bucketAssigner.onNewBatch(metadata.getCluster(), table.getNumBuckets(), bucket);
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

  private void produce(int bucket, List<byte[]> batches) throws IOException {
    if (closed) throw new IOException("Fluss Arrow client is closed");
    var records = Unpooled.wrappedBuffer(batches.toArray(byte[][]::new));
    try {
      var request =
          new ProduceLogRequest()
              .setTableId(table.getTableId())
              .setAcks(-1)
              .setTimeoutMs(Math.toIntExact(config.get(CLIENT_REQUEST_TIMEOUT).toMillis()));
      request.addBucketsReq().setBucketId(bucket).setRecords(records);
      long produceStart = profile ? System.nanoTime() : 0;
      var response =
          retry(
              new TableBucket(table.getTableId(), bucket),
              gateway -> {
                if (profile) produceRequests.increment();
                return gateway.produceLog(request);
              },
              reply -> validateAppendResponse(reply, bucket),
              config.get(CLIENT_WRITER_RETRIES));
      if (profile) produceRpcNanos.add(System.nanoTime() - produceStart);
      if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
        response.getParsedByteBuf().release();
    } finally {
      records.release();
    }
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

  @FunctionalInterface
  private interface ResponseValidator<T> {
    void validate(T response) throws IOException;
  }

  private <T extends ApiMessage> T retry(
      TableBucket bucket,
      java.util.function.Function<TabletServerGateway, CompletableFuture<T>> call,
      ResponseValidator<T> validator,
      int maxRetries)
      throws IOException {
    for (long attempt = 0; ; attempt++) {
      try {
        metadata.checkAndUpdateMetadata(path, bucket);
        var gateway = metadata.newTabletServerClientForNode(metadata.leaderFor(path, bucket));
        if (gateway == null)
          throw new org.apache.fluss.exception.LeaderNotAvailableException(
              "Fluss bucket leader is unavailable");
        T response = await(call.apply(gateway));
        try {
          validator.validate(response);
          return response;
        } catch (IOException | RuntimeException | Error failure) {
          if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
            response.getParsedByteBuf().release();
          throw failure;
        }
      } catch (RetriableException failure) {
        if (attempt >= maxRetries) throw failure;
        metadata.updateTableOrPartitionMetadata(path, bucket.getPartitionId());
        try {
          Thread.sleep(100);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IOException("Fluss retry interrupted", interrupted);
        }
      }
    }
  }

  private static void checkError(org.apache.fluss.rpc.messages.ErrorMessage response) {
    if (response.hasErrorCode()) {
      RuntimeException error =
          Errors.forCode(response.getErrorCode())
              .exception(response.hasErrorMessage() ? response.getErrorMessage() : null);
      if (error != null) throw error;
    }
  }

  static <T> T await(CompletableFuture<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException failure) {
      // The shared transport can outlive this reader; release a reply arriving after cancellation.
      future.thenAccept(
          reply -> {
            if (reply instanceof ApiMessage message
                && message.isLazilyParsed()
                && message.getParsedByteBuf() != null) message.getParsedByteBuf().release();
          });
      Thread.currentThread().interrupt();
      throw new IOException(failure);
    } catch (ExecutionException failure) {
      if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
      throw new IOException(failure.getCause());
    }
  }

  @Override
  public void close() throws Exception {
    closed = true;
    if (producers != null) producers.shutdownNow();
    try {
      if (remoteDownloader != null) remoteDownloader.close();
    } finally {
      try {
        connectionLease.close();
      } finally {
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
  }
}
