package tech.streamfusion.fluss;

import static org.apache.fluss.config.ConfigOptions.*;
import static org.apache.fluss.record.LogRecordBatchFormat.*;
import static tech.streamfusion.fluss.FlussArrowContext.await;
import static tech.streamfusion.fluss.FlussArrowContext.checkError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.log.RemoteLogDownloader;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.rpc.messages.*;
import org.apache.fluss.rpc.util.CommonRpcMessageUtils;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import tech.streamfusion.fluss.FlussArrowClient.Fetched;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Fetch/decode owner. Closing transport never reclaims Arrow roots already returned to callers. */
final class FlussArrowReader implements AutoCloseable {
  private final FlussArrowContext context;
  private final Schema outputSchema;
  private final int[] projectedFields;
  private final PbPredicate filter;
  private final Map<Integer, Schema> schemas = new HashMap<>();
  private RemoteLogDownloader remoteDownloader;
  long fetchRpcNanos;
  long decodeNanos;
  long receivedRecordBytes;
  long borrowedRecordBytes;
  private volatile boolean closed;

  FlussArrowReader(
      FlussArrowContext context,
      Schema outputSchema,
      boolean projectRead,
      Predicate recordBatchFilter)
      throws IOException {
    this.context = context;
    this.outputSchema = outputSchema;
    filter =
        recordBatchFilter == null
            ? null
            : PredicateMessageUtils.toPbPredicate(recordBatchFilter, context.table.getRowType());
    if (outputSchema.getFields().isEmpty()
        && context
            .table
            .getRowType()
            .getFieldNames()
            .contains(RowDataArrowConverter.ROW_KIND_COLUMN))
      throw new IOException("Fluss physical schema collides with the changelog sidecar");
    schemas.put(context.table.getSchemaId(), context.wireSchema);
    // Released 1.0 schemas start at 1 and evolve only by appending nullable columns.
    // Broker projection is positional, so old batches cannot supply newly added positions.
    boolean prune = projectRead && context.table.getSchemaId() == 1;
    int[] selectedFields =
        java.util.stream.IntStream.range(0, context.table.getRowType().getFieldCount())
            .filter(
                i ->
                    !prune
                        || outputSchema.getFields().stream()
                            .anyMatch(
                                field ->
                                    field
                                        .getName()
                                        .equals(
                                            context
                                                .table
                                                .getRowType()
                                                .getFields()
                                                .get(i)
                                                .getName())))
            .toArray();
    if (selectedFields.length == 0 && !outputSchema.getFields().isEmpty())
      throw new IOException("Fluss projection contains no physical table columns");
    // Fluss 1.0 cannot serialize a zero-field broker projection. Retain one wire column;
    // local schema conversion drops it while preserving row counts and changelog kinds.
    projectedFields = selectedFields.length == 0 ? new int[] {0} : selectedFields;
  }

  public List<Fetched> fetch(LogSplit split) throws IOException {
    if (closed) throw new IOException("Fluss Arrow client is closed");
    TableBucket bucket = split.getTableBucket();
    context.metadata.checkAndUpdateMetadata(context.path, bucket);
    int bucketCount =
        context
            .metadata
            .getCluster()
            .getBucketCountOrElseThrow(
                TableOrPartition.of(bucket.getTableId(), bucket.getPartitionId()));
    long start = split.getStartingOffset();
    boolean resolveEarliest =
        start == org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
    if (resolveEarliest) {
      var offsets =
          bucket.getPartitionId() == null
              ? context
                  .connection
                  .getAdmin()
                  .listOffsets(
                      context.path,
                      List.of(bucket.getBucket()),
                      new org.apache.fluss.client.admin.OffsetSpec.EarliestSpec())
              : context
                  .connection
                  .getAdmin()
                  .listOffsets(
                      context.path,
                      split.getPartitionName(),
                      List.of(bucket.getBucket()),
                      new org.apache.fluss.client.admin.OffsetSpec.EarliestSpec());
      start = await(offsets.all()).get(bucket.getBucket());
    }
    if (split.getStoppingOffset().isPresent() && start >= split.getStoppingOffset().get())
      return List.of(new Fetched(null, split.getStoppingOffset().get()));
    FetchLogRequest request =
        new FetchLogRequest()
            .setFollowerServerId(-1)
            .setReadPreference(context.config.get(CLIENT_SCANNER_LOG_READ_PREFERENCE).value())
            .setMaxBytes(
                Math.toIntExact(context.config.get(CLIENT_SCANNER_LOG_FETCH_MAX_BYTES).getBytes()))
            .setMinBytes(
                Math.toIntExact(context.config.get(CLIENT_SCANNER_LOG_FETCH_MIN_BYTES).getBytes()))
            .setMaxWaitMs(
                Math.toIntExact(
                    context.config.get(CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME).toMillis()));
    var requestTable = request.addTablesReq().setTableId(bucket.getTableId());
    if (filter != null) {
      requestTable.setFilterPredicate(filter);
      requestTable.setFilterSchemaId(context.table.getSchemaId());
    }
    boolean projected = projectedFields.length < context.table.getRowType().getFieldCount();
    requestTable.setProjectionPushdownEnabled(projected);
    if (projected) requestTable.setProjectedFields(projectedFields);
    var requestBucket =
        requestTable
            .addBucketsReq()
            .setBucketId(bucket.getBucket())
            .setRoutingBucketCount(bucketCount)
            .setFetchOffset(start)
            .setMaxFetchBytes(
                Math.toIntExact(
                    context.config.get(CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET).getBytes()));
    if (bucket.getPartitionId() != null) requestBucket.setPartitionId(bucket.getPartitionId());
    long rpcStart = context.profile ? System.nanoTime() : 0;
    FetchLogResponse response =
        context.retry(
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
                  || bucketResponse.getBucketId() != bucket.getBucket()
                  || !java.util.Objects.equals(
                      bucketResponse.hasPartitionId() ? bucketResponse.getPartitionId() : null,
                      bucket.getPartitionId())) {
                throw new IOException("Unexpected Fluss fetch reply identity");
              }
              checkError(bucketResponse);
            },
            Integer.MAX_VALUE);
    if (context.profile) fetchRpcNanos += System.nanoTime() - rpcStart;
    long decodeStart = context.profile ? System.nanoTime() : 0;
    List<Fetched> result = new ArrayList<>();
    try {
      var bucketResponse = response.getTablesRespAt(0).getBucketsRespAt(0);
      var fetch =
          CommonRpcMessageUtils.getFetchLogResultForBucket(bucket, context.path, bucketResponse);
      long stop =
          Math.min(split.getStoppingOffset().orElse(Long.MAX_VALUE), fetch.getHighWatermark());
      if (fetch.fetchFromRemote()) {
        fetchRemote(fetch.remoteLogFetchInfo(), start, stop, result);
      } else if (bucketResponse.hasRecords()) {
        ByteBuffer records = CommonRpcMessageUtils.toByteBuffer(bucketResponse.getRecordsSlice());
        try (ArrowBuf received =
            FlussReceiveBuffer.borrow(response.getParsedByteBuf(), context.allocator)) {
          var slice = bucketResponse.getRecordsSlice();
          ArrowBuf borrowedRecords =
              received != null && slice.hasMemoryAddress()
                  ? received.slice(
                      slice.memoryAddress() + slice.readerIndex() - received.memoryAddress(),
                      slice.readableBytes())
                  : null;
          if (context.profile) {
            receivedRecordBytes += records.remaining();
            if (borrowedRecords != null) borrowedRecordBytes += records.remaining();
          }
          decodeRecords(records, start, stop, projected, true, borrowedRecords, result);
        }
      }
      if (fetch.hasFilteredEndOffset())
        result.add(new Fetched(null, Math.min(fetch.getFilteredEndOffset(), stop)));
      if (result.isEmpty() && resolveEarliest) result.add(new Fetched(null, start));
      return result;
    } catch (Throwable failure) {
      result.forEach(Fetched::close);
      throw failure;
    } finally {
      if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
        response.getParsedByteBuf().release();
      if (context.profile) decodeNanos += System.nanoTime() - decodeStart;
    }
  }

  private void fetchRemote(RemoteLogFetchInfo info, long start, long stop, List<Fetched> result)
      throws IOException {
    RemoteLogDownloader downloader = remoteDownloader();
    boolean first = true;
    for (var segment : info.remoteLogSegmentList()) {
      var future = downloader.requestRemoteLog(new FsPath(info.remoteLogTabletDir()), segment);
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

  private synchronized RemoteLogDownloader remoteDownloader() throws IOException {
    if (closed) throw new IOException("Fluss Arrow client is closed");
    if (remoteDownloader == null) {
      remoteDownloader =
          new RemoteLogDownloader(
              "streamfusion",
              context.config,
              context.connection.getOrCreateRemoteFileDownloader(),
              new ScannerMetricGroup(context.connection.getClientMetricGroup(), context.path));
      remoteDownloader.start();
    }
    return remoteDownloader;
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
              context.allocator,
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
                        FlussArrowSchema.convert(decoded.root(), outputSchema, context.allocator),
                        next));
              } else {
                try (VectorSchemaRoot view = decoded.root().slice(from, count)) {
                  output.add(
                      new Fetched(
                          FlussArrowSchema.convert(view, outputSchema, context.allocator), next));
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
      var info = await(context.connection.getAdmin().getTableSchema(context.path, id));
      schema = FlussArrowSchema.wireSchema(info.getSchema().getRowType());
      schemas.put(id, schema);
    }
    return schema;
  }

  @Override
  public void close() throws Exception {
    RemoteLogDownloader downloader;
    synchronized (this) {
      if (closed) return;
      closed = true;
      downloader = remoteDownloader;
    }
    if (downloader != null) downloader.close();
  }
}
