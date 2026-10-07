package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class FlussArrowClientTest {
  private static FlussTestCluster cluster;
  private static Connection connection;
  private static final Schema PROJECTION =
      new Schema(List.of(Field.nullable("value", new ArrowType.Int(64, true))));

  @BeforeAll
  static void start() throws Exception {
    cluster = new FlussTestCluster();
    cluster.start();
    try {
      connection = ConnectionFactory.createConnection(cluster.config());
    } catch (Exception failure) {
      System.err.println(cluster.logs());
      throw failure;
    }
    connection.getAdmin().createDatabase("arrow_test", DatabaseDescriptor.EMPTY, true).get();
  }

  @AfterAll
  static void stop() throws Exception {
    try {
      if (connection != null) connection.close();
    } finally {
      if (cluster != null) cluster.close();
    }
  }

  private static TablePath table(String name, boolean pk, String compression, String image)
      throws Exception {
    TablePath path = TablePath.of("arrow_test", name);
    var schema =
        org.apache.fluss.metadata.Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT());
    if (pk) schema.primaryKey("id");
    connection
        .getAdmin()
        .createTable(
            path,
            TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(1)
                .property("table.log.format", "ARROW")
                .property("table.log.arrow.compression.type", compression)
                .property("table.changelog.image", image)
                .build(),
            false)
        .get();
    return path;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 4})
  void stickyBatchesRemainTogetherAcrossBuckets(int buckets) throws Exception {
    TablePath path = TablePath.of("arrow_test", "sticky_" + buckets);
    connection
        .getAdmin()
        .createTable(
            path,
            TableDescriptor.builder()
                .schema(
                    org.apache.fluss.metadata.Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("value", DataTypes.BIGINT())
                        .build())
                .distributedBy(buckets)
                .property("table.log.arrow.compression.type", "NONE")
                .build(),
            false)
        .get();
    int batches = 32;
    int[] assigned = new int[batches];
    java.util.Arrays.fill(assigned, -1);
    try (RootAllocator allocator = new RootAllocator();
        var writer =
            new FlussArrowClient(
                cluster.config(),
                path,
                FlussArrowSchema.wireSchema(
                    connection.getAdmin().getTableInfo(path).get().getRowType()),
                allocator,
                false);
        var root =
            VectorSchemaRoot.create(
                FlussArrowSchema.wireSchema(writer.tableInfo().getRowType()), allocator);
        var scanner = connection.getTable(path).newScan().createLogScanner()) {
      root.allocateNew();
      for (int batch = 0; batch < batches; batch++) {
        for (int row = 0; row < 2; row++) {
          ((BigIntVector) root.getVector(0)).setSafe(row, batch);
          ((BigIntVector) root.getVector(1)).setSafe(row, row);
        }
        root.setRowCount(2);
        writer.appendAsync(root);
      }
      writer.flush();
      for (int bucket = 0; bucket < buckets; bucket++) scanner.subscribe(bucket, 0);
      int count = 0;
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (count < batches * 2 && System.nanoTime() < deadline) {
        var records = scanner.poll(Duration.ofSeconds(1));
        for (var bucket : records.buckets()) {
          for (var record : records.records(bucket)) {
            int batch = Math.toIntExact(record.getRow().getLong(0));
            if (assigned[batch] != -1) assertEquals(assigned[batch], bucket.getBucket());
            assigned[batch] = bucket.getBucket();
            count++;
          }
        }
      }
      assertEquals(batches * 2, count);
      if (buckets > 1)
        for (int batch = 1; batch < batches; batch++)
          assertNotEquals(assigned[batch - 1], assigned[batch]);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"NONE", "LZ4_FRAME", "ZSTD"})
  void arrowAppendIsReadableByStockClientAndProjectedArrowClient(String compression)
      throws Exception {
    TablePath path = table("append_" + compression.toLowerCase(), false, compression, "FULL");
    Schema inputSchema =
        new Schema(
            List.of(
                Field.nullable("query_id", new ArrowType.Int(64, true)),
                Field.nullable("query_value", new ArrowType.Int(64, true))));
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(inputSchema, allocator);
        FlussArrowClient writer =
            new FlussArrowClient(cluster.config(), path, inputSchema, allocator, false)) {
      root.allocateNew();
      ((BigIntVector) root.getVector(0)).setSafe(0, 7);
      ((BigIntVector) root.getVector(1)).setSafe(0, 42);
      ((BigIntVector) root.getVector(0)).setSafe(1, 8);
      ((BigIntVector) root.getVector(1)).setNull(1);
      root.setRowCount(2);
      writer.append(root);
      try (var scanner = connection.getTable(path).newScan().createLogScanner()) {
        scanner.subscribe(0, 0);
        List<Long> values = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (values.size() < 2 && System.nanoTime() < deadline) {
          for (var record : scanner.poll(Duration.ofSeconds(1)))
            values.add(record.getRow().isNullAt(1) ? null : record.getRow().getLong(1));
        }
        assertEquals(java.util.Arrays.asList(42L, null), values);
      }
      try (FlussArrowClient reader =
          new FlussArrowClient(cluster.config(), path, PROJECTION, allocator)) {
        var bucket = new TableBucket(writer.tableInfo().getTableId(), 0);
        List<String> records = read(reader, bucket, 0, 2);
        assertEquals(List.of("0:42", "0:null"), records);
        assertEquals(List.of("0:null"), read(reader, bucket, 1, 2));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"FULL", "WAL"})
  void projectedPrimaryKeyLogPreservesStockChangelogAndOffsets(String image) throws Exception {
    TablePath path = table("cdc_" + image.toLowerCase(), true, "ZSTD", image);
    var writer = connection.getTable(path).newUpsert().createWriter();
    writer.upsert(GenericRow.of(1L, 10L)).get();
    writer.upsert(GenericRow.of(1L, 20L)).get();
    writer.delete(GenericRow.of(1L, null)).get();
    writer.flush();
    List<String> expected = new ArrayList<>();
    long stop = 0;
    try (var scanner = connection.getTable(path).newScan().createLogScanner()) {
      scanner.subscribe(0, 0);
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (expected.size() < 3 && System.nanoTime() < deadline) {
        for (var record : scanner.poll(Duration.ofSeconds(1))) {
          int kind =
              switch (record.getChangeType()) {
                case APPEND_ONLY, INSERT -> 0;
                case UPDATE_BEFORE -> 1;
                case UPDATE_AFTER -> 2;
                case DELETE -> 3;
              };
          expected.add(
              kind + ":" + (record.getRow().isNullAt(1) ? null : record.getRow().getLong(1)));
          stop = record.logOffset() + 1;
        }
      }
    }
    assertEquals(image.equals("FULL") ? 4 : 3, expected.size());
    try (RootAllocator allocator = new RootAllocator();
        FlussArrowClient reader =
            new FlussArrowClient(cluster.config(), path, PROJECTION, allocator)) {
      TableBucket bucket = new TableBucket(reader.tableInfo().getTableId(), 0);
      assertEquals(expected, read(reader, bucket, 0, stop));
    }
  }

  @Test
  void receiveBufferCountersVerifyTheRealBrokerBorrowingPath() throws Exception {
    TablePath path = table("receive_buffers", false, "NONE", "FULL");
    var writer = connection.getTable(path).newAppend().createWriter();
    List<String> expected = new ArrayList<>();
    for (long id = 0; id < 128; id++) {
      writer.append(GenericRow.of(id, id * 10));
      expected.add("0:" + id * 10);
    }
    writer.flush();
    Schema projected = new Schema(List.of(Field.nullable("value", new ArrowType.Int(64, true))));
    String oldProfile = System.getProperty("streamfusion.fluss.profile");
    String oldDirect = System.getProperty("streamfusion.fluss.direct-receive.enabled");
    System.setProperty("streamfusion.fluss.profile", "true");
    try {
      for (boolean direct : new boolean[] {false, true}) {
        System.setProperty("streamfusion.fluss.direct-receive.enabled", Boolean.toString(direct));
        try (var allocator = new RootAllocator();
            var reader = new FlussArrowClient(cluster.config(), path, projected, allocator)) {
          assertEquals(
              expected, read(reader, new TableBucket(reader.tableInfo().getTableId(), 0), 0, 128));
          var profile = reader.transportProfile();
          assertTrue(profile.receivedRecordBytes() > 0);
          assertEquals(direct ? profile.receivedRecordBytes() : 0, profile.borrowedRecordBytes());
        }
      }
    } finally {
      restoreProperty("streamfusion.fluss.profile", oldProfile);
      restoreProperty("streamfusion.fluss.direct-receive.enabled", oldDirect);
    }
  }

  private static void restoreProperty(String key, String previous) {
    if (previous == null) System.clearProperty(key);
    else System.setProperty(key, previous);
  }

  @Test
  void sharedTransportClosesOnlyAfterItsLastOwner() throws Exception {
    var config = cluster.config();
    var initial = java.util.Map.copyOf(config.toMap());
    var first = FlussConnections.acquire(config);
    var second = FlussConnections.acquire(new org.apache.fluss.config.Configuration(config));
    try {
      assertSame(first.connection(), second.connection());
      assertEquals(
          initial, config.toMap(), "The Java client must not mutate the cache key's caller");
      first.close();
      first.close();
      assertNotNull(second.connection().getAdmin().listDatabases().get());
      var different = new org.apache.fluss.config.Configuration(config);
      different.set(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME, Duration.ofMillis(1));
      try (var separate = FlussConnections.acquire(different)) {
        assertNotSame(second.connection(), separate.connection());
      }
    } finally {
      first.close();
      second.close();
    }
    try (var replacement = FlussConnections.acquire(config)) {
      assertNotSame(second.connection(), replacement.connection(), "No idle connections survive");
      assertNotNull(replacement.connection().getAdmin().listDatabases().get());
    }
  }

  @Test
  void immediateShutdownWaitsForThreadsAndPreservesBorrowedVectors() throws Exception {
    String previous = System.getProperty("streamfusion.fluss.fast-close.enabled");
    System.setProperty("streamfusion.fluss.fast-close.enabled", "true");
    try {
      TablePath path = table("immediate_shutdown", false, "NONE", "FULL");
      Schema full =
          new Schema(
              List.of(
                  Field.nullable("id", new ArrowType.Int(64, true)),
                  Field.nullable("value", new ArrowType.Int(64, true))));
      var config = cluster.config();
      try (RootAllocator allocator = new RootAllocator()) {
        TableBucket bucket;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(full, allocator);
            FlussArrowClient writer = new FlussArrowClient(config, path, full, allocator, false)) {
          root.allocateNew();
          ((BigIntVector) root.getVector(0)).setSafe(0, 1);
          ((BigIntVector) root.getVector(1)).setSafe(0, 42);
          root.setRowCount(1);
          writer.append(root);
          writer.flush();
          bucket = new TableBucket(writer.tableInfo().getTableId(), 0);
        }
        try (var lease = FlussConnections.acquire(config);
            FlussArrowClient reader = new FlussArrowClient(config, path, PROJECTION, allocator)) {
          var group = FlussDirectReceive.bootstrap(lease.connection()).config().group();
          var fetched = reader.fetch(new LogSplit(bucket, null, 0, 1));
          try {
            lease.close();
            assertFalse(group.isShuttingDown(), "Another reader still owns the connection");
            reader.close();
            assertTrue(group.isTerminated(), "Last-owner close must finish network teardown");
            var root =
                fetched.stream()
                    .filter(batch -> batch.root() != null)
                    .findFirst()
                    .orElseThrow()
                    .root();
            assertEquals(1, root.getRowCount());
            assertEquals(
                42,
                ((BigIntVector) root.getVector(0)).get(0),
                "Borrowed Arrow allocations outlive the network event loop");
          } finally {
            fetched.forEach(FlussArrowClient.Fetched::close);
          }
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    } finally {
      if (previous == null) System.clearProperty("streamfusion.fluss.fast-close.enabled");
      else System.setProperty("streamfusion.fluss.fast-close.enabled", previous);
    }
  }

  @Test
  void checkpointRestoresOnlyCollectedRowsAndReleasesPrefetchedBatches() throws Exception {
    TablePath path = table("recovery", false, "ZSTD", "FULL");
    Schema full =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("value", new ArrowType.Int(64, true))));
    TableBucket bucket;
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(full, allocator);
        FlussArrowClient writer =
            new FlussArrowClient(cluster.config(), path, full, allocator, false)) {
      root.allocateNew();
      for (int first : new int[] {1, 3, 4}) {
        int count = first == 3 ? 1 : 2;
        for (int i = 0; i < count; i++) {
          ((BigIntVector) root.getVector(0)).setSafe(i, first + i);
          ((BigIntVector) root.getVector(1)).setSafe(i, (first + i) * 10);
        }
        root.setRowCount(count);
        writer.append(root);
      }
      bucket = new TableBucket(writer.tableInfo().getTableId(), 0);
    }
    var type =
        (org.apache.flink.table.types.logical.RowType)
            org.apache.flink.table.api.DataTypes.ROW(
                    org.apache.flink.table.api.DataTypes.FIELD(
                        "value", org.apache.flink.table.api.DataTypes.BIGINT()))
                .getLogicalType();
    List<Long> collected = new ArrayList<>();
    List<org.apache.fluss.flink.source.split.SourceSplitBase> checkpoint;
    try (FlussArrowSourceReader reader =
        new FlussArrowSourceReader(readerContext(), cluster.config(), path, type, -1, null)) {
      reader.start();
      reader.addSplits(List.of(new LogSplit(bucket, null, 0, 5)));
      reader.notifyNoMoreSplits();
      reader.isAvailable().get(30, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(0, reader.snapshotState(1).get(0).asLogSplit().getStartingOffset());
      reader.pollNext(readerOutput(collected, false));
      checkpoint = reader.snapshotState(2);
      assertEquals(2, checkpoint.get(0).asLogSplit().getStartingOffset());
      assertEquals(List.of(10L, 20L), collected);
    }
    try (FlussArrowSourceReader reader =
        new FlussArrowSourceReader(readerContext(), cluster.config(), path, type, -1, null)) {
      reader.start();
      reader.addSplits(checkpoint);
      reader.notifyNoMoreSplits();
      org.apache.flink.core.io.InputStatus status;
      do {
        reader.isAvailable().get(30, java.util.concurrent.TimeUnit.SECONDS);
        status = reader.pollNext(readerOutput(collected, false));
      } while (status != org.apache.flink.core.io.InputStatus.END_OF_INPUT);
      assertTrue(reader.snapshotState(3).isEmpty());
    }
    assertEquals(List.of(10L, 20L, 30L, 40L, 50L), collected);
    try (FlussArrowSourceReader reader =
        new FlussArrowSourceReader(readerContext(), cluster.config(), path, type, -1, null)) {
      reader.addSplits(List.of(new LogSplit(bucket, null, 0, 5)));
      reader.isAvailable().get(30, java.util.concurrent.TimeUnit.SECONDS);
      assertThrows(
          IllegalStateException.class, () -> reader.pollNext(readerOutput(collected, true)));
      assertEquals(0, reader.snapshotState(4).get(0).asLogSplit().getStartingOffset());
    }
    try (FlussArrowSourceReader reader =
        new FlussArrowSourceReader(readerContext(), cluster.config(), path, type, -1, null)) {
      reader.addSplits(List.of(new LogSplit(bucket, null, 0, 5)));
      reader.isAvailable().get(30, java.util.concurrent.TimeUnit.SECONDS);
      assertThrows(
          IllegalStateException.class, () -> reader.pollNext(readerOutput(collected, false, true)));
      assertEquals(0, reader.snapshotState(5).get(0).asLogSplit().getStartingOffset());
    }
  }

  @Test
  void limitedFetchResumesBeforeAnIncompleteTrailingBatch() throws Exception {
    TablePath path = table("fetch_limit", false, "NONE", "FULL");
    Schema full =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("value", new ArrowType.Int(64, true))));
    var config = cluster.config();
    config.setString("client.scanner.log.fetch.max-bytes", "200kb");
    config.setString("client.scanner.log.fetch.max-bytes-for-bucket", "200kb");
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(full, allocator);
        FlussArrowClient writer = new FlussArrowClient(config, path, full, allocator, false);
        FlussArrowClient reader = new FlussArrowClient(config, path, full, allocator)) {
      root.allocateNew();
      for (int i = 0; i < 8192; i++) {
        ((BigIntVector) root.getVector(0)).setSafe(i, i);
        ((BigIntVector) root.getVector(1)).setSafe(i, i);
      }
      root.setRowCount(8192);
      for (int i = 0; i < 4; i++) writer.append(root);
      long offset = 0;
      long rows = 0;
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      TableBucket bucket = new TableBucket(writer.tableInfo().getTableId(), 0);
      while (offset < 32768 && System.nanoTime() < deadline) {
        for (var fetched : reader.fetch(new LogSplit(bucket, null, offset, 32768))) {
          try (fetched) {
            if (fetched.root() != null) rows += fetched.root().getRowCount();
            assertTrue(fetched.nextOffset() > offset);
            offset = fetched.nextOffset();
          }
        }
      }
      assertEquals(32768, offset);
      assertEquals(32768, rows);
    }
  }

  @Test
  void oversizedArrowInputIsSplitWithinTheWriterRequestLimit() throws Exception {
    TablePath path = table("split_append", false, "NONE", "FULL");
    Schema full =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("value", new ArrowType.Int(64, true))));
    var config = cluster.config();
    config.set(ConfigOptions.CLIENT_WRITER_REQUEST_MAX_SIZE, MemorySize.parse("8kb"));
    config.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("32kb"));
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(full, allocator);
        var writer = new FlussArrowClient(config, path, full, allocator, false);
        var reader = new FlussArrowClient(cluster.config(), path, PROJECTION, allocator)) {
      root.allocateNew();
      for (int i = 0; i < 8192; i++) {
        ((BigIntVector) root.getVector(0)).setSafe(i, i);
        ((BigIntVector) root.getVector(1)).setSafe(i, i * 31L);
      }
      root.setRowCount(8192);
      writer.append(root);
      assertEquals(8192, root.getRowCount());
      assertEquals(8191L, root.getVector(0).getObject(8191));
      var bucket = new TableBucket(writer.tableInfo().getTableId(), 0);
      assertEquals(
          java.util.stream.IntStream.range(0, 8192).mapToObj(i -> "0:" + i * 31L).toList(),
          read(reader, bucket, 0, 8192));
    }
  }

  @Test
  void retryingAFiveBatchRequestDoesNotAppendDuplicates() throws Exception {
    TablePath path = table("group_retry", false, "NONE", "FULL");
    var info = connection.getAdmin().getTableInfo(path).get();
    var metadata = ((FlussConnection) connection).getMetadataUpdater();
    var bucket = new TableBucket(info.getTableId(), 0);
    metadata.checkAndUpdateMetadata(path, bucket);
    var gateway = metadata.newTabletServerClientForNode(metadata.leaderFor(path, bucket));
    long writerId = gateway.initWriter(new InitWriterRequest()).get().getWriterId();
    Schema full = FlussArrowSchema.wireSchema(info.getRowType());
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(full, allocator)) {
      root.allocateNew();
      root.setRowCount(1);
      byte[][] batches = new byte[5][];
      for (int i = 0; i < batches.length; i++) {
        ((BigIntVector) root.getVector(0)).setSafe(0, i);
        ((BigIntVector) root.getVector(1)).setSafe(0, i * 10L);
        batches[i] = FlussArrowLogBatch.encode(root, info.getSchemaId(), writerId, i);
      }
      var records = Unpooled.wrappedBuffer(batches);
      try {
        var request =
            new ProduceLogRequest().setTableId(info.getTableId()).setAcks(-1).setTimeoutMs(30000);
        request.addBucketsReq().setBucketId(0).setRecords(records);
        for (int attempt = 0; attempt < 3; attempt++) {
          for (int retry = 0; ; retry++) {
            var response = gateway.produceLog(request).get();
            try {
              var result = response.getBucketsRespAt(0);
              int error = result.hasErrorCode() ? result.getErrorCode() : 0;
              if (error == org.apache.fluss.rpc.protocol.Errors.NOT_LEADER_OR_FOLLOWER.code()
                  && retry < 100) {
                Thread.sleep(50);
                metadata.checkAndUpdateMetadata(path, bucket);
                gateway = metadata.newTabletServerClientForNode(metadata.leaderFor(path, bucket));
                continue;
              }
              assertEquals(0, error, "replay " + attempt);
              break;
            } finally {
              if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
                response.getParsedByteBuf().release();
            }
          }
        }
      } finally {
        records.release();
      }
      assertEquals(
          5L,
          connection
              .getAdmin()
              .listOffsets(path, List.of(0), new OffsetSpec.LatestSpec())
              .bucketResult(0)
              .get());
      try (var reader = new FlussArrowClient(cluster.config(), path, PROJECTION, allocator)) {
        assertEquals(List.of("0:0", "0:10", "0:20", "0:30", "0:40"), read(reader, bucket, 0, 5));
      }
    }
  }

  @Test
  void asynchronousAppendSnapshotsInputAndFlushWaitsForDurableAcknowledgements() throws Exception {
    TablePath path = table("async_flush", false, "NONE", "FULL");
    Schema full =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("value", new ArrowType.Int(64, true))));
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(full, allocator);
        FlussArrowClient writer =
            new FlussArrowClient(cluster.config(), path, full, allocator, false)) {
      root.allocateNew();
      ((BigIntVector) root.getVector(0)).setSafe(0, 0);
      ((BigIntVector) root.getVector(1)).setSafe(0, 0);
      root.setRowCount(1);
      writer.append(root);
      cluster.pauseTablet();
      boolean paused = true;
      try {
        var id = (BigIntVector) root.getVector(0);
        var value = (BigIntVector) root.getVector(1);
        List<java.util.concurrent.CompletableFuture<Void>> pending = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
          id.setSafe(0, i);
          value.setSafe(0, i * 10);
          pending.add(writer.appendAsync(root));
        }
        id.setSafe(0, -1);
        value.setSafe(0, -1);
        assertTrue(pending.stream().noneMatch(java.util.concurrent.CompletableFuture::isDone));
        var flushed =
            executor.submit(
                () -> {
                  writer.flush();
                  return null;
                });
        assertThrows(
            java.util.concurrent.TimeoutException.class,
            () -> flushed.get(200, java.util.concurrent.TimeUnit.MILLISECONDS));
        cluster.resumeTablet();
        paused = false;
        flushed.get(30, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(pending.stream().allMatch(java.util.concurrent.CompletableFuture::isDone));
      } finally {
        if (paused) cluster.resumeTablet();
      }
      try (FlussArrowClient reader =
          new FlussArrowClient(cluster.config(), path, PROJECTION, allocator)) {
        var bucket = new TableBucket(writer.tableInfo().getTableId(), 0);
        List<String> expected =
            java.util.stream.IntStream.rangeClosed(0, 12).mapToObj(i -> "0:" + (i * 10)).toList();
        assertEquals(expected, read(reader, bucket, 0, 13));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void addedNullableColumnsReadHistoricalArrowBatches(boolean primaryKey) throws Exception {
    TablePath path = table("evolution_" + primaryKey, primaryKey, "ZSTD", "FULL");
    assertEquals(1, connection.getAdmin().getTableInfo(path).get().getSchemaId());
    var table = connection.getTable(path);
    if (primaryKey) {
      var writer = table.newUpsert().createWriter();
      writer.upsert(GenericRow.of(1L, 10L)).get();
      writer.flush();
    } else {
      var writer = table.newAppend().createWriter();
      writer.append(GenericRow.of(1L, 10L)).get();
      writer.flush();
    }
    connection
        .getAdmin()
        .alterTable(
            path,
            List.of(
                org.apache.fluss.metadata.TableChange.addColumn(
                    "later",
                    DataTypes.BIGINT(),
                    null,
                    org.apache.fluss.metadata.TableChange.ColumnPosition.last())),
            false)
        .get();
    try (Connection fresh = ConnectionFactory.createConnection(cluster.config())) {
      var evolved = fresh.getTable(path);
      if (primaryKey) {
        var writer = evolved.newUpsert().createWriter();
        writer.upsert(GenericRow.of(1L, 20L, 200L)).get();
        writer.flush();
      } else {
        var writer = evolved.newAppend().createWriter();
        writer.append(GenericRow.of(2L, 20L, 200L)).get();
        writer.flush();
      }
      List<String> expected = new ArrayList<>();
      long stop = 0;
      try (var scanner = evolved.newScan().createLogScanner()) {
        scanner.subscribe(0, 0);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (expected.size() < (primaryKey ? 3 : 2) && System.nanoTime() < deadline) {
          for (var record : scanner.poll(Duration.ofSeconds(1))) {
            expected.add(
                record.getChangeType()
                    + ":"
                    + (record.getRow().isNullAt(2) ? null : record.getRow().getLong(2)));
            stop = record.logOffset() + 1;
          }
        }
      }
      Schema projection = new Schema(List.of(Field.nullable("later", new ArrowType.Int(64, true))));
      List<String> actual = new ArrayList<>();
      try (RootAllocator allocator = new RootAllocator();
          FlussArrowClient reader =
              new FlussArrowClient(cluster.config(), path, projection, allocator)) {
        long offset = 0;
        TableBucket bucket = new TableBucket(reader.tableInfo().getTableId(), 0);
        while (offset < stop) {
          for (var fetched : reader.fetch(new LogSplit(bucket, null, offset, stop))) {
            try (fetched) {
              if (fetched.root() != null) {
                var kinds =
                    (TinyIntVector) fetched.root().getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
                for (int i = 0; i < fetched.root().getRowCount(); i++) {
                  String kind =
                      kinds == null
                          ? "APPEND_ONLY"
                          : switch (kinds.get(i)) {
                            case 0 -> "INSERT";
                            case 1 -> "UPDATE_BEFORE";
                            case 2 -> "UPDATE_AFTER";
                            default -> throw new AssertionError("Unexpected evolution change type");
                          };
                  actual.add(kind + ":" + fetched.root().getVector("later").getObject(i));
                }
              }
              offset = fetched.nextOffset();
            }
          }
        }
      }
      assertEquals(primaryKey ? 3 : 2, expected.size());
      assertEquals(expected, actual);
    }
  }

  private static org.apache.flink.api.connector.source.SourceReaderContext readerContext() {
    org.apache.flink.configuration.Configuration config =
        new org.apache.flink.configuration.Configuration();
    config.set(
        org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
        org.apache.flink.configuration.MemorySize.parse("48g"));
    return (org.apache.flink.api.connector.source.SourceReaderContext)
        java.lang.reflect.Proxy.newProxyInstance(
            FlussArrowClientTest.class.getClassLoader(),
            new Class<?>[] {org.apache.flink.api.connector.source.SourceReaderContext.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getConfiguration" -> config;
                  case "metricGroup" ->
                      org.apache.flink.metrics.groups.UnregisteredMetricsGroup
                          .createSourceReaderMetricGroup();
                  case "sendSplitRequest", "sendSourceEventToCoordinator" -> null;
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  @SuppressWarnings("unchecked")
  private static org.apache.flink.api.connector.source.ReaderOutput<
          tech.streamfusion.operator.ArrowBatch>
      readerOutput(List<Long> collected, boolean fail) {
    return readerOutput(collected, fail, false);
  }

  @SuppressWarnings("unchecked")
  private static org.apache.flink.api.connector.source.ReaderOutput<
          tech.streamfusion.operator.ArrowBatch>
      readerOutput(List<Long> collected, boolean fail, boolean failAfterConsume) {
    return (org.apache.flink.api.connector.source.ReaderOutput<
            tech.streamfusion.operator.ArrowBatch>)
        java.lang.reflect.Proxy.newProxyInstance(
            FlussArrowClientTest.class.getClassLoader(),
            new Class<?>[] {org.apache.flink.api.connector.source.ReaderOutput.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "createOutputForSplit":
                  return proxy;
                case "releaseOutputForSplit", "emitWatermark", "markIdle", "markActive":
                  return null;
                case "collect":
                  if (fail) throw new IllegalStateException("Injected downstream failure");
                  try (var root = ((tech.streamfusion.operator.ArrowBatch) args[0]).root()) {
                    for (int i = 0; i < root.getRowCount(); i++)
                      collected.add((Long) root.getVector("value").getObject(i));
                  }
                  if (failAfterConsume)
                    throw new IllegalStateException("Injected failure after consuming root");
                  return null;
                default:
                  throw new UnsupportedOperationException(method.getName());
              }
            });
  }

  private static List<String> read(
      FlussArrowClient reader, TableBucket bucket, long start, long stop) throws Exception {
    List<String> result = new ArrayList<>();
    long offset = start;
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (offset < stop && System.nanoTime() < deadline) {
      for (var fetched : reader.fetch(new LogSplit(bucket, null, offset, stop))) {
        try (fetched) {
          if (fetched.root() != null) {
            assertNotNull(fetched.root().getVector("value"));
            assertNull(fetched.root().getVector("id"));
            TinyIntVector kinds =
                (TinyIntVector) fetched.root().getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
            for (int i = 0; i < fetched.root().getRowCount(); i++)
              result.add(
                  (kinds == null ? 0 : kinds.get(i))
                      + ":"
                      + fetched.root().getVector("value").getObject(i));
          }
          assertTrue(fetched.nextOffset() > offset);
          offset = fetched.nextOffset();
        }
      }
    }
    assertEquals(stop, offset);
    return result;
  }
}
