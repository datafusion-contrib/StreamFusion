package tech.streamfusion.operator;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.types.logical.RowType;

final class CanonicalOverSnapshotTestSupport {
  private CanonicalOverSnapshotTestSupport() {}

  static OperatorSubtaskState snapshot(
      AbstractStreamOperatorTestHarness<?> harness, long id, long time) throws Exception {
    var owner =
        harness.getOperator() instanceof DirectOver ? (DirectOver) harness.getOperator() : null;
    long before = owner == null ? 0 : owner.rawSnapshots.get();
    var saved = tech.streamfusion.CanonicalTestSnapshots.snapshot(harness, id, time);
    return verifyDirect(owner, before, saved);
  }

  static Capture capture(AbstractStreamOperatorTestHarness<?> harness, long id) throws Exception {
    var owner = (DirectOver) harness.getOperator();
    long before = owner.rawSnapshots.get();
    return new Capture(owner, before, CanonicalDistinctWindowAsyncTest.capture(harness, id));
  }

  private static OperatorSubtaskState verifyDirect(
      DirectOver owner, long before, OperatorSubtaskState saved) throws Exception {
    if (owner != null && owner.rawSnapshots.get() != before) {
      saved.discardState();
      throw new AssertionError("Canonical OVER capture used an intermediate native snapshot");
    }
    return saved;
  }

  static final class Capture implements AutoCloseable {
    private final DirectOver owner;
    private final long before;
    private final OperatorSnapshotFutures futures;

    private Capture(DirectOver owner, long before, OperatorSnapshotFutures futures) {
      this.owner = owner;
      this.before = before;
      this.futures = futures;
    }

    OperatorSubtaskState finish() throws Exception {
      return verifyDirect(owner, before, CanonicalDistinctWindowAsyncTest.finishOnWriter(futures));
    }

    @Override
    public void close() throws Exception {
      futures.cancel();
    }
  }

  static final class DirectOver extends NativeOverAggregateOperator {
    private final AtomicLong rawSnapshots = new AtomicLong();
    private transient boolean canonical;

    DirectOver(
        int timeColumn,
        int[] valueColumns,
        int[] keyColumns,
        int[] valueTypes,
        int[] aggregateKinds,
        int frameKind,
        long frameOffset,
        boolean proctime,
        int[] keyTimestampPrecisions,
        long stateTtlMillis,
        RowType rowType,
        int maxParallelism) {
      super(
          timeColumn,
          valueColumns,
          keyColumns,
          valueTypes,
          aggregateKinds,
          frameKind,
          frameOffset,
          proctime,
          keyTimestampPrecisions,
          stateTtlMillis,
          rowType,
          maxParallelism);
    }

    @Override
    protected OperatorSnapshotFutures snapshotCanonicalState(
        long id, long time, CheckpointOptions options, CheckpointStreamFactory streams)
        throws Exception {
      canonical = true;
      try {
        return super.snapshotCanonicalState(id, time, options, streams);
      } finally {
        canonical = false;
      }
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      rawSnapshots.incrementAndGet();
      if (canonical) throw new AssertionError("Canonical OVER capture serialized native raw state");
      return super.snapshotRawPartitions();
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      rawSnapshots.incrementAndGet();
      if (canonical)
        throw new AssertionError("Canonical OVER capture serialized native partitions");
      return super.snapshotCanonicalPartitions();
    }

    @Override
    protected String[] checkpointRocksDBHandle(String directory) {
      if (!directory.isEmpty()) {
        rawSnapshots.incrementAndGet();
        if (canonical)
          throw new AssertionError("Canonical OVER capture created a native checkpoint");
      }
      return super.checkpointRocksDBHandle(directory);
    }
  }
}
