package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeIcebergKeyedTest {
  private static final int INITIAL_ROWS = 3;

  @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS)
  Path temp;

  private static final AtomicBoolean FAILED = new AtomicBoolean();
  private static final AtomicBoolean RESTORED = new AtomicBoolean();
  private static final java.util.Queue<String> EVENTS =
      new java.util.concurrent.ConcurrentLinkedQueue<>();

  @ParameterizedTest(name = "unaligned recovery repetition={0}")
  @org.junit.jupiter.params.provider.MethodSource("recoveryRepetitions")
  void unalignedRecoveryKeepsUpdatesUnderRepeatedCheckpointRaces(int repetition) throws Exception {
    hostDeletesAndRecoveryPreserveNativeDataPositions(repetition % 2 == 0, true, "hash", true, 2);
  }

  static java.util.stream.IntStream recoveryRepetitions() {
    return java.util.stream.IntStream.rangeClosed(
        1, Integer.getInteger("sf.iceberg.recovery.repetitions", 20));
  }

  @Test
  void partialInitialSnapshotDoesNotReleaseUpdates() {
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("partial").toUri().toString());
    assertFalse(initialRowsCommitted(table));
    for (int row = 0; row < INITIAL_ROWS; row++) {
      table
          .newAppend()
          .appendFile(
              DataFiles.builder(table.spec())
                  .withPath(temp.resolve("initial-" + row + ".parquet").toString())
                  .withFileSizeInBytes(1)
                  .withRecordCount(1)
                  .build())
          .commit();
      assertNotNull(table.currentSnapshot());
      assertEquals(row + 1 == INITIAL_ROWS, initialRowsCommitted(table));
    }
  }

  private static boolean initialRowsCommitted(Table table) {
    var snapshot = table.currentSnapshot();
    return snapshot != null
        && Integer.toString(INITIAL_ROWS)
            .equals(snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP));
  }

  @Test
  void recoveryBoundaryRequiresCompletionOfACheckpointTakenAfterCommit() throws Exception {
    var source = new PhasedSource(false, "unused");
    source.snapshots = new ConcurrentHashMap<>();
    source.snapshots.put(10L, 1);
    source.latestCompletedId = 9;
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      var waiting =
          executor.submit(
              () -> {
                source.awaitRecoveryBoundary(10, System.nanoTime() + 5_000_000_000L);
                return null;
              });
      source.notifyCheckpointComplete(10);
      assertThrows(
          java.util.concurrent.TimeoutException.class,
          () -> waiting.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
      source.snapshots.put(11L, 1);
      assertThrows(
          java.util.concurrent.TimeoutException.class,
          () -> waiting.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
      source.notifyCheckpointComplete(11);
      waiting.get(5, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      source.cancel();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
    }
  }

  @Test
  void missingRecoveryBoundaryFailsWithSourceEvents() {
    var source = new PhasedSource(false, "unused");
    source.latestCompletedId = 10;
    var failure =
        assertThrows(
            IllegalStateException.class, () -> source.awaitRecoveryBoundary(10, System.nanoTime()));
    assertTrue(failure.getMessage().contains("committed recovery boundary timed out"));
  }

  @Test
  void emittedUpdatesAdvanceRecoveryProgressBeforeTheInjectedFailure() throws Exception {
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("emitted-progress").toUri().toString());
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(temp.resolve("initial.parquet").toString())
                .withFileSizeInBytes(1)
                .withRecordCount(INITIAL_ROWS)
                .build())
        .commit();
    var source = new PhasedSource(true, table.location());
    source.next = 1;
    source.completed = 1;
    source.latestSnapshotId = 10;
    source.latestCompletedId = 11;
    var emitted = new java.util.concurrent.atomic.AtomicInteger();
    Object lock = new Object();
    SourceFunction.SourceContext<ArrowBatch> context =
        new SourceFunction.SourceContext<>() {
          @Override
          public void collect(ArrowBatch batch) {
            assertTrue(Thread.holdsLock(lock));
            try (var root = batch.root()) {
              emitted.addAndGet(root.getRowCount());
            }
          }

          @Override
          public void collectWithTimestamp(ArrowBatch batch, long timestamp) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void markAsTemporarilyIdle() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object getCheckpointLock() {
            return lock;
          }

          @Override
          public void close() {}
        };
    FAILED.set(false);
    try {
      var failure = assertThrows(IllegalStateException.class, () -> source.run(context));
      assertTrue(failure.getMessage().contains("injected failure"));
      assertEquals(6, emitted.get());
      synchronized (lock) {
        assertEquals(2, source.next, "a checkpoint after failure must not replay emitted updates");
      }
    } finally {
      FAILED.set(false);
      source.cancel();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,none,false,2",
    "true,false,none,false,2",
    "false,true,hash,false,2",
    "true,true,hash,false,2",
    "false,true,hash,true,2",
    "true,true,hash,true,2",
    "false,false,range,false,2",
    "true,false,range,false,2",
    "false,false,none,false,3",
    "true,false,none,false,3",
    "false,true,hash,false,3",
    "true,true,hash,false,3",
    "false,true,hash,true,3",
    "true,true,hash,true,3",
    "false,false,range,false,3",
    "true,false,range,false,3"
  })
  void hostDeletesAndRecoveryPreserveNativeDataPositions(
      boolean upsert, boolean fail, String distribution, boolean unaligned, int formatVersion)
      throws Exception {
    FAILED.set(false);
    RESTORED.set(false);
    EVENTS.clear();
    String location = temp.resolve("keyed").toUri().toString();
    var schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "value", Types.IntegerType.get())),
            java.util.Set.of(1));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.builderFor(schema).bucket("id", 4).build(),
                Map.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)),
                location);
    var configuration =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "fixed-delay",
                "restart-strategy.fixed-delay.attempts", "1",
                "restart-strategy.fixed-delay.delay", "10 ms"));
    configuration.set(
        org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY,
        temp.resolve("checkpoints").toUri().toString());
    var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(2);
    env.setMaxParallelism(16);
    env.enableCheckpointing(100);
    if (unaligned) env.getCheckpointConfig().enableUnalignedCheckpoints();
    var input =
        env.addSource(new PhasedSource(fail, location))
            .returns(ArrowBatchTypeInformation.INSTANCE)
            .setParallelism(1);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(location),
        configuration,
        Map.of(
            "distribution-mode",
            distribution,
            "upsert-enabled",
            Boolean.toString(upsert),
            "write-parallelism",
            "2"));
    var job = env.executeAsync("native-iceberg-keyed-checkpoints");
    try {
      job.getJobExecutionResult().get(60, java.util.concurrent.TimeUnit.SECONDS);
    } catch (Exception failure) {
      try {
        job.cancel().get();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
    table.refresh();
    var actual = new java.util.TreeMap<Integer, Integer>();
    try (var rows = IcebergGenerics.read(table).build()) {
      for (var row : rows) {
        assertNull(
            actual.put((Integer) row.getField("id"), (Integer) row.getField("value")),
            () -> "Duplicate row " + row + "; " + recoveryDiagnostics(table));
      }
    }
    assertEquals(Map.of(1, 11, 3, 30, 5, 50), actual, () -> recoveryDiagnostics(table));
    assertTrue(
        java.util.stream.StreamSupport.stream(table.snapshots().spliterator(), false).count() >= 2);
    var deleteKinds = java.util.EnumSet.noneOf(org.apache.iceberg.FileContent.class);
    for (var snapshot : table.snapshots()) {
      if (formatVersion == 3) {
        assertNotNull(snapshot.firstRowId());
        assertNotNull(snapshot.addedRows());
      }
      for (var delete : snapshot.addedDeleteFiles(table.io())) {
        deleteKinds.add(delete.content());
        if (delete.format() == org.apache.iceberg.FileFormat.PUFFIN) {
          assertEquals(3, formatVersion);
          assertEquals(org.apache.iceberg.FileContent.POSITION_DELETES, delete.content());
          assertNotNull(delete.referencedDataFile());
          assertTrue(delete.contentOffset() >= 0);
          assertTrue(delete.contentSizeInBytes() > 0);
          continue;
        }
        if (formatVersion == 3)
          assertEquals(org.apache.iceberg.FileContent.EQUALITY_DELETES, delete.content());
        try (var parquet =
            org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
                org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(delete.location()),
                    new org.apache.hadoop.conf.Configuration()))) {
          assertTrue(parquet.getFooter().getFileMetaData().getCreatedBy().contains("parquet-mr"));
        }
      }
    }
    assertEquals(
        java.util.Set.of(
            org.apache.iceberg.FileContent.EQUALITY_DELETES,
            org.apache.iceberg.FileContent.POSITION_DELETES),
        deleteKinds,
        () -> recoveryDiagnostics(table));
    if (formatVersion == 3) {
      try (var tasks = table.newScan().planFiles()) {
        for (var task : tasks) assertNotNull(task.file().firstRowId());
      }
    }
    assertEquals(fail, FAILED.get());
    assertEquals(fail, RESTORED.get());
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static String recoveryDiagnostics(Table table) {
    return "Source events="
        + EVENTS
        + "; table="
        + table.location()
        + "; snapshots="
        + java.util.stream.StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(snapshot -> snapshot.snapshotId() + ":" + snapshot.summary())
            .toList();
  }

  private static ArrowBatch batch(int phase) {
    int[] keys = phase == 0 ? new int[] {1, 2, 3} : new int[] {1, 1, 2, 4, 4, 5};
    int[] values = phase == 0 ? new int[] {10, 20, 30} : new int[] {10, 11, 20, 40, 40, 50};
    RowKind[] kinds =
        phase == 0
            ? new RowKind[] {RowKind.INSERT, RowKind.INSERT, RowKind.INSERT}
            : new RowKind[] {
              RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE,
              RowKind.INSERT, RowKind.DELETE, RowKind.UPDATE_AFTER
            };
    var id = new IntVector("id", NativeAllocator.SHARED);
    var value = new IntVector("value", NativeAllocator.SHARED);
    var kind = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
    var root = VectorSchemaRoot.of(id, value, kind);
    root.allocateNew();
    for (int i = 0; i < keys.length; i++) {
      id.setSafe(i, keys[i]);
      value.setSafe(i, values[i]);
      kind.setSafe(i, kinds[i].toByteValue());
    }
    root.setRowCount(keys.length);
    return new ArrowBatch(root);
  }

  private static final class PhasedSource
      implements SourceFunction<ArrowBatch>, CheckpointedFunction, CheckpointListener {
    private final boolean fail;
    private volatile boolean running = true;
    private volatile int completed;
    private int next;
    private transient ListState<Integer> state;
    private transient Map<Long, Integer> snapshots;
    private transient volatile long latestSnapshotId;
    private transient volatile long latestCompletedId;

    private final String location;

    private PhasedSource(boolean fail, String location) {
      this.fail = fail;
      this.location = location;
    }

    @Override
    public void run(SourceContext<ArrowBatch> context) throws Exception {
      if (next == 0) {
        synchronized (context.getCheckpointLock()) {
          context.collect(batch(0));
          next = 1;
        }
      }
      long deadline = System.nanoTime() + java.time.Duration.ofSeconds(45).toNanos();
      while (running && completed < 1 && next < 2) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("checkpoint timed out");
        Thread.sleep(5);
      }
      if (!running) return;
      if (next < 2) {
        var initial = new HadoopTables(new org.apache.hadoop.conf.Configuration()).load(location);
        // Unaligned checkpoints can commit one writer while other initial rows remain in flight.
        while (running && !initialRowsCommitted(initial)) {
          if (System.nanoTime() > deadline)
            throw new IllegalStateException("initial checkpoint commit timed out");
          Thread.sleep(5);
          initial.refresh();
        }
        if (!running) return;
        // A committed snapshot may belong to a checkpoint that still retained initial channel
        // records. Recover from a later checkpoint taken after those records were committed.
        long committedAfter = latestSnapshotId;
        EVENTS.add("initial rows committed after snapshot " + committedAfter);
        awaitRecoveryBoundary(committedAfter, deadline);
        if (!running) return;
        EVENTS.add("recovery boundary " + latestCompletedId);
        synchronized (context.getCheckpointLock()) {
          EVENTS.add("emit updates");
          context.collect(batch(1));
          // A queued checkpoint can acquire this lock before the source failure reaches the task.
          next = 2;
          if (fail && FAILED.compareAndSet(false, true)) {
            throw new IllegalStateException("injected failure after an uncommitted update batch");
          }
        }
      }
      while (running && completed < 2) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("checkpoint timed out");
        Thread.sleep(5);
      }
    }

    private void awaitRecoveryBoundary(long committedAfter, long deadline)
        throws InterruptedException {
      while (running && latestCompletedId <= committedAfter) {
        if (System.nanoTime() > deadline)
          throw new IllegalStateException("committed recovery boundary timed out; " + EVENTS);
        Thread.sleep(5);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(List.of(next));
      snapshots.put(context.getCheckpointId(), next);
      latestSnapshotId = context.getCheckpointId();
      EVENTS.add("snapshot " + context.getCheckpointId() + "=" + next);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("iceberg-arrow-source-phase", Integer.class));
      snapshots = new ConcurrentHashMap<>();
      latestSnapshotId = -1;
      latestCompletedId = -1;
      if (context.isRestored()) {
        for (int phase : state.get()) next = phase;
        completed = next;
        RESTORED.set(true);
        EVENTS.add("restore " + next);
      }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      EVENTS.add("complete " + checkpointId + "=" + snapshots.getOrDefault(checkpointId, 0));
      completed = Math.max(completed, snapshots.getOrDefault(checkpointId, 0));
      latestCompletedId = checkpointId;
      snapshots.keySet().removeIf(id -> id <= checkpointId);
    }
  }
}
