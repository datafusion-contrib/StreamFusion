package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CoordinatorCommittingRowDataStoreWriteOperator;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.flink.sink.coordinator.RestoredCommittableEvent;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Paimon's pending-committable recovery protocol with stock rows and native Arrow batches. */
class PaimonCoordinatorCommitRecoveryTest {

  private static final String COMMIT_USER = "coordinated-writer";
  private static final RowType ROW_TYPE = RowType.of(new IntType(), new BigIntType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(60)
  void recoveryReplaysPendingCommitsExactlyOnce(boolean committedBeforeFailure) throws Exception {
    FileStoreTable stock = runRecovery(false, committedBeforeFailure);
    FileStoreTable ours = runRecovery(true, committedBeforeFailure);
    assertEquals(List.of("1|10", "2|20", "3|30"), PaimonTestTables.readRows(ours, ours.rowType()));
    assertEquals(
        PaimonTestTables.readRows(stock, stock.rowType()),
        PaimonTestTables.readRows(ours, ours.rowType()));
    assertEquals(snapshots(stock), snapshots(ours));
  }

  private static FileStoreTable runRecovery(boolean nativeWriter, boolean committedBeforeFailure)
      throws Exception {
    FileStoreTable table = createTable();
    List<OperatorEvent> events = new ArrayList<>();
    OperatorSubtaskState saved;
    byte[] coordinatorState;
    MockOperatorCoordinatorContext context =
        new MockOperatorCoordinatorContext(new OperatorID(), 1);
    try (var coordinator = coordinator(table, context);
        var harness = harness(table, nativeWriter, events::add)) {
      coordinator.start();
      coordinator.waitProcessAllActions();
      harness.open();
      feed(harness, nativeWriter, 1, 2);
      harness.processWatermark(new Watermark(100));
      harness.prepareSnapshotPreBarrier(1);
      harness.snapshot(1, 10);
      // Preserve an empty, idle checkpoint alongside the pending data checkpoint.
      harness.processWatermark(new Watermark(200));
      harness.processWatermarkStatus(WatermarkStatus.IDLE);
      harness.prepareSnapshotPreBarrier(2);
      saved = harness.snapshot(2, 20);
      deliver(coordinator, events);
      CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
      coordinator.checkpointCoordinator(2, checkpoint);
      coordinatorState = checkpoint.get();
      if (committedBeforeFailure) {
        coordinator.notifyCheckpointComplete(2);
        coordinator.waitProcessAllActions();
        assertEquals(2, PaimonTestTables.readRows(table, table.rowType()).size());
        // The writer never receives the completion acknowledgement, so it must replay these.
      } else {
        assertTrue(table.latestSnapshot().isEmpty());
      }
      feed(harness, nativeWriter, 999);
      harness.prepareSnapshotPreBarrier(3);
      harness.snapshot(3, 30);
      // Checkpoint 3 is lost; its files must never appear in a snapshot after restoration.
      events.clear();
      assertFalse(context.isJobFailed(), String.valueOf(context.getJobFailureReason()));
    }
    for (int attempt = 0; attempt < (committedBeforeFailure ? 1 : 2); attempt++) {
      MockOperatorCoordinatorContext restoredContext =
          new MockOperatorCoordinatorContext(new OperatorID(), 1);
      try (var coordinator = coordinator(table, restoredContext);
          var harness = harness(table, nativeWriter, events::add)) {
        coordinator.resetToCheckpoint(2, coordinatorState);
        coordinator.start();
        coordinator.waitProcessAllActions();
        TaskStateSnapshot taskState = new TaskStateSnapshot();
        taskState.putSubtaskStateByOperatorID(harness.getOperator().getOperatorID(), saved);
        ((TestTaskStateManager) harness.getEnvironment().getTaskStateManager())
            .restoreLatestCheckpointState(Map.of(2L, taskState));
        harness.initializeEmptyState();
        harness.open();
        assertEquals(1, events.size());
        assertInstanceOf(RestoredCommittableEvent.class, events.get(0));
        deliver(coordinator, events);
        assertEquals(2, PaimonTestTables.readRows(table, table.rowType()).size());
        if (!committedBeforeFailure && attempt == 0) {
          // Paimon deliberately restarts once after recovering a previously uncommitted snapshot.
          assertTrue(restoredContext.isJobFailed());
          assertTrue(
              restoredContext
                  .getJobFailureReason()
                  .toString()
                  .contains("intentionally thrown after committing"));
          continue;
        }
        assertFalse(
            restoredContext.isJobFailed(),
            restoredContext.getJobFailureReason() + " snapshots=" + snapshots(table));
        feed(harness, nativeWriter, 3);
        harness.processWatermark(new Watermark(300));
        harness.prepareSnapshotPreBarrier(4);
        harness.snapshot(4, 40);
        deliver(coordinator, events);
        coordinator.notifyCheckpointComplete(4);
        coordinator.waitProcessAllActions();
        harness.notifyOfCompletedCheckpoint(4);
        assertFalse(
            restoredContext.isJobFailed(), String.valueOf(restoredContext.getJobFailureReason()));
        assertEquals(300, table.latestSnapshot().get().watermark());
      }
    }
    return table;
  }

  private static CommittingWriteOperatorCoordinator coordinator(
      FileStoreTable table, MockOperatorCoordinatorContext context) {
    return new CommittingWriteOperatorCoordinator(
        context,
        commitContext ->
            new StoreCommitter(
                table,
                table.newCommit(commitContext.commitUser()).ignoreEmptyCommit(false),
                commitContext),
        true,
        COMMIT_USER,
        true);
  }

  private static void deliver(
      CommittingWriteOperatorCoordinator coordinator, List<OperatorEvent> events) throws Exception {
    for (OperatorEvent event : events) {
      coordinator.handleEventFromOperator(0, 0, event);
    }
    coordinator.waitProcessAllActions();
    events.clear();
  }

  private static OneInputStreamOperatorTestHarness<Object, Committable> harness(
      FileStoreTable table, boolean nativeWriter, OperatorEventGateway gateway) throws Exception {
    var harness =
        new OneInputStreamOperatorTestHarness<Object, Committable>(
            new TestFactory(table, nativeWriter, gateway));
    harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
    return harness;
  }

  private static void feed(
      OneInputStreamOperatorTestHarness<Object, Committable> harness,
      boolean nativeWriter,
      int... ids)
      throws Exception {
    if (nativeWriter) {
      try (var allocator = new RootAllocator()) {
        List<org.apache.flink.table.data.RowData> rows = new ArrayList<>();
        for (int id : ids) {
          rows.add(GenericRowData.of(id, id * 10L));
        }
        harness.processElement(
            new StreamRecord<>(
                new BucketedArrowBatch(
                    RowDataArrowConverter.write(rows, ROW_TYPE, allocator),
                    BinaryRow.EMPTY_ROW.toBytes(),
                    0)));
      }
    } else {
      for (int id : ids) {
        harness.processElement(new StreamRecord<>(GenericRow.of(id, id * 10L)));
      }
    }
  }

  private static FileStoreTable createTable() throws Exception {
    Path path = new Path(Files.createTempDirectory("paimon-coordinator-recovery").toUri());
    new SchemaManager(LocalFileIO.create(), path)
        .createTable(
            Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("v", DataTypes.BIGINT())
                .options(Map.of("bucket", "-1", "write-only", "true", "file.format", "parquet"))
                .build());
    return FileStoreTableFactory.create(LocalFileIO.create(), path);
  }

  private static List<String> snapshots(FileStoreTable table) throws Exception {
    List<String> snapshots = new ArrayList<>();
    var iterator = table.snapshotManager().snapshots();
    while (iterator.hasNext()) {
      var snapshot = iterator.next();
      snapshots.add(
          snapshot.id()
              + ":"
              + snapshot.commitIdentifier()
              + ":"
              + snapshot.commitKind()
              + ":"
              + snapshot.totalRecordCount()
              + ":"
              + snapshot.watermark());
    }
    return snapshots;
  }

  private static final class TestFactory extends AbstractStreamOperatorFactory<Committable>
      implements OneInputStreamOperatorFactory<Object, Committable> {
    private final FileStoreTable table;
    private final boolean nativeWriter;
    private final OperatorEventGateway gateway;

    TestFactory(FileStoreTable table, boolean nativeWriter, OperatorEventGateway gateway) {
      this.table = table;
      this.nativeWriter = nativeWriter;
      this.gateway = gateway;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Committable>> T createStreamOperator(
        StreamOperatorParameters<Committable> parameters) {
      StoreSinkWrite.Provider provider =
          StoreSinkWrite.createWriteProvider(table, new CheckpointConfig(), true, false, false);
      return (T)
          (nativeWriter
              ? new NativePaimonCoordinatorCommitOperator(
                  parameters, table, provider, COMMIT_USER, gateway)
              : new CoordinatorCommittingRowDataStoreWriteOperator(
                  parameters, table, provider, COMMIT_USER, gateway));
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return nativeWriter
          ? NativePaimonCoordinatorCommitOperator.class
          : CoordinatorCommittingRowDataStoreWriteOperator.class;
    }
  }
}
