package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Reconstructs destination-local parent batches after the unaligned-safe shuffle emits one record
 * per key group. Incomplete parents are union state. After rescaling, old-attempt fragments remain
 * independently processable because some siblings may already live in downstream operator state;
 * the new producer attempt gets a fresh epoch and resumes ordered parent reassembly.
 */
public final class OrderedKeyGroupReassembler extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, BoundedOneInput {
  private static final ListStateDescriptor<byte[]> STATE =
      new ListStateDescriptor<>(
          "streamfusion-ordered-key-group-fragments", BytePrimitiveArraySerializer.INSTANCE);
  private static final ListStateDescriptor<Long> WATERMARK_STATE =
      new ListStateDescriptor<>(
          "streamfusion-ordered-key-group-watermark", LongSerializer.INSTANCE);
  private static final ListStateDescriptor<byte[]> EPOCH_STATE =
      new ListStateDescriptor<>(
          "streamfusion-ordered-key-group-epochs", BytePrimitiveArraySerializer.INSTANCE);

  private final int maxParallelism;
  private final Map<ParentId, PartialParent> parents = new HashMap<>();
  private final Set<Epoch> seenEpochs = new HashSet<>();
  private final Set<Epoch> recoveryEpochs = new HashSet<>();
  private final List<ArrowBatch> restoredFragments = new ArrayList<>();
  private transient ListState<byte[]> fragmentState;
  private transient ListState<Long> watermarkState;
  private transient ListState<byte[]> epochState;
  private transient ArrowBatchSerializer serializer;
  private long pendingWatermark = Long.MIN_VALUE;
  private boolean restoredWatermarkPending;

  public OrderedKeyGroupReassembler(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    serializer = new ArrowBatchSerializer();
    fragmentState = context.getOperatorStateStore().getUnionListState(STATE);
    watermarkState = context.getOperatorStateStore().getUnionListState(WATERMARK_STATE);
    epochState = context.getOperatorStateStore().getUnionListState(EPOCH_STATE);
    if (context.isRestored()) {
      for (byte[] bytes : epochState.get()) {
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        Epoch epoch = new Epoch(input.readLong(), input.readLong());
        recoveryEpochs.add(epoch);
        seenEpochs.add(epoch);
      }
      for (long watermark : watermarkState.get()) {
        restoredWatermarkPending = true;
        pendingWatermark =
            pendingWatermark == Long.MIN_VALUE
                ? watermark
                : Math.min(pendingWatermark, watermark);
      }
      for (byte[] bytes : fragmentState.get()) {
        ArrowBatch fragment = serializer.deserialize(new DataInputDeserializer(bytes));
        if (owns(fragment.keyGroup())) {
          restoredFragments.add(fragment);
        } else {
          fragment.root().close();
        }
      }
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    restoredFragments.sort(
        Comparator.comparingLong(ArrowBatch::parentSequence)
            .thenComparingInt(fragment -> fragment.rowOrdinals()[0]));
    for (ArrowBatch fragment : restoredFragments) {
      ColumnarRecordMetrics.emit(output, getMetricGroup(), fragment);
    }
    restoredFragments.clear();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    ArrowBatch fragment = element.getValue();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), fragment.rowCount());
    if (!fragment.isOrderedKeyGroupFragment()) {
      ColumnarRecordMetrics.forward(
          output, getMetricGroup(), element, fragment.rowCount());
      return;
    }
    Epoch epoch = new Epoch(fragment.parentEpochHigh(), fragment.parentEpochLow());
    seenEpochs.add(epoch);
    if (recoveryEpochs.contains(epoch)) {
      ColumnarRecordMetrics.forward(
          output, getMetricGroup(), element, fragment.rowCount());
      return;
    }
    add(fragment, true);
  }

  private void add(ArrowBatch fragment, boolean emitWhenComplete) throws Exception {
    if (fragment.parentKeyGroups() == null || fragment.parentKeyGroups().length == 0) {
      fragment.root().close();
      throw new IllegalArgumentException("Ordered fragment must carry its parent key groups");
    }
    ParentId id =
        new ParentId(
            fragment.parentEpochHigh(), fragment.parentEpochLow(), fragment.parentSequence());
    PartialParent parent =
        parents.computeIfAbsent(
            id,
            ignored ->
                new PartialParent(
                    fragment.parentKeyGroups(),
                    expectedFragments(fragment.parentKeyGroups())));
    if (!Arrays.equals(parent.keyGroups, fragment.parentKeyGroups())) {
      fragment.root().close();
      throw new IllegalStateException(
          "Inconsistent key-group list for parent " + id);
    }
    ArrowBatch replaced = parent.fragments.putIfAbsent(fragment.keyGroup(), fragment);
    if (replaced != null) {
      fragment.root().close();
      throw new IllegalStateException(
          "Duplicate key-group fragment " + fragment.keyGroup() + " for parent " + id);
    }
    if (emitWhenComplete && parent.isComplete()) {
      parents.remove(id);
      emit(id, parent);
    }
  }

  private boolean owns(int keyGroup) {
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
            maxParallelism,
            tech.streamfusion.compat.RuntimeCompat.parallelism(getRuntimeContext()),
            keyGroup)
        == tech.streamfusion.compat.RuntimeCompat.subtask(getRuntimeContext());
  }

  private int expectedFragments(int[] keyGroups) {
    int expected = 0;
    for (int keyGroup : keyGroups) {
      if (owns(keyGroup)) {
        expected++;
      }
    }
    return expected;
  }

  private void emit(ParentId id, PartialParent parent) {
    if (!parent.isComplete()) {
      throw new IllegalStateException("Incomplete ordered parent " + id);
    }
    int representativeKeyGroup = Integer.MAX_VALUE;
    for (ArrowBatch fragment : parent.fragments.values()) {
      representativeKeyGroup = Math.min(representativeKeyGroup, fragment.keyGroup());
    }
    ArrowBatch first = parent.fragments.values().iterator().next();
    VectorSchemaRoot firstRoot = first.root();
    VectorSchemaRoot merged = VectorSchemaRoot.create(firstRoot.getSchema(), NativeAllocator.SHARED);
    merged.allocateNew();
    Map<ArrowBatch, VectorSchemaRoot> roots = new HashMap<>();
    roots.put(first, firstRoot);
    PriorityQueue<RowCursor> rows =
        new PriorityQueue<>(Comparator.comparingInt(RowCursor::ordinal));
    try {
      for (ArrowBatch fragment : parent.fragments.values()) {
        if (fragment != first) {
          roots.put(fragment, fragment.root());
        }
        if (fragment.rowOrdinals().length > 0) {
          rows.add(new RowCursor(fragment, 0, fragment.rowOrdinals()[0]));
        }
      }
      int destinationRow = 0;
      while (!rows.isEmpty()) {
        RowCursor row = rows.remove();
        VectorSchemaRoot source = roots.get(row.fragment());
        for (int column = 0; column < merged.getFieldVectors().size(); column++) {
          FieldVector targetVector = merged.getVector(column);
          targetVector.copyFromSafe(row.sourceRow(), destinationRow, source.getVector(column));
        }
        destinationRow++;
        int nextSourceRow = row.sourceRow() + 1;
        if (nextSourceRow < row.fragment().rowOrdinals().length) {
          rows.add(
              new RowCursor(
                  row.fragment(),
                  nextSourceRow,
                  row.fragment().rowOrdinals()[nextSourceRow]));
        }
      }
      for (FieldVector vector : merged.getFieldVectors()) {
        vector.setValueCount(destinationRow);
      }
      merged.setRowCount(destinationRow);
    } catch (Throwable failure) {
      merged.close();
      throw failure;
    } finally {
      roots.values().forEach(VectorSchemaRoot::close);
    }
    ColumnarRecordMetrics.emit(
        output, getMetricGroup(), new ArrowBatch(merged, representativeKeyGroup));
    if (parents.isEmpty()
        && pendingWatermark != Long.MIN_VALUE
        && !restoredWatermarkPending) {
      output.emitWatermark(new Watermark(pendingWatermark));
      pendingWatermark = Long.MIN_VALUE;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    fragmentState.clear();
    watermarkState.clear();
    epochState.clear();
    for (PartialParent parent : parents.values()) {
      for (ArrowBatch fragment : parent.fragments.values()) {
        DataOutputSerializer bytes = new DataOutputSerializer(256);
        serializer.serialize(fragment.retainedCopy(), bytes);
        fragmentState.add(bytes.getCopyOfBuffer());
      }
    }
    if (pendingWatermark != Long.MIN_VALUE) {
      watermarkState.add(pendingWatermark);
    }
    for (Epoch epoch : seenEpochs) {
      DataOutputSerializer bytes = new DataOutputSerializer(16);
      bytes.writeLong(epoch.high());
      bytes.writeLong(epoch.low());
      epochState.add(bytes.getCopyOfBuffer());
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    seenEpochs.removeAll(recoveryEpochs);
    recoveryEpochs.clear();
    if (parents.isEmpty()) {
      pendingWatermark = Long.MIN_VALUE;
      restoredWatermarkPending = false;
      super.processWatermark(mark);
    } else {
      pendingWatermark = Math.max(pendingWatermark, mark.getTimestamp());
    }
  }

  @Override
  public void endInput() {
    seenEpochs.removeAll(recoveryEpochs);
    recoveryEpochs.clear();
    if (!parents.isEmpty()) {
      throw new IllegalStateException("End of input with incomplete key-group parents: " + parents);
    }
    if (pendingWatermark != Long.MIN_VALUE) {
      output.emitWatermark(new Watermark(pendingWatermark));
      pendingWatermark = Long.MIN_VALUE;
      restoredWatermarkPending = false;
    }
  }

  @Override
  public void close() throws Exception {
    for (ArrowBatch fragment : restoredFragments) {
      fragment.root().close();
    }
    restoredFragments.clear();
    for (PartialParent parent : parents.values()) {
      for (ArrowBatch fragment : parent.fragments.values()) {
        fragment.root().close();
      }
    }
    parents.clear();
    super.close();
  }

  private static final class ParentId {
    private final long epochHigh;
    private final long epochLow;
    private final long sequence;

    private ParentId(long epochHigh, long epochLow, long sequence) {
      this.epochHigh = epochHigh;
      this.epochLow = epochLow;
      this.sequence = sequence;
    }

    public long epochHigh() {
      return epochHigh;
    }

    public long epochLow() {
      return epochLow;
    }

    public long sequence() {
      return sequence;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      ParentId that = (ParentId) other;
      return epochHigh == that.epochHigh && epochLow == that.epochLow && sequence == that.sequence;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + Long.hashCode(epochHigh);
      result = 31 * result + Long.hashCode(epochLow);
      result = 31 * result + Long.hashCode(sequence);
      return result;
    }

    @Override
    public String toString() {
      return "ParentId[epochHigh="
          + epochHigh
          + ", epochLow="
          + epochLow
          + ", sequence="
          + sequence
          + "]";
    }
  }

  private static final class Epoch {
    private final long high;
    private final long low;

    private Epoch(long high, long low) {
      this.high = high;
      this.low = low;
    }

    public long high() {
      return high;
    }

    public long low() {
      return low;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Epoch that = (Epoch) other;
      return high == that.high && low == that.low;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + Long.hashCode(high);
      result = 31 * result + Long.hashCode(low);
      return result;
    }

    @Override
    public String toString() {
      return "Epoch[high=" + high + ", low=" + low + "]";
    }
  }

  private static final class RowCursor {
    private final ArrowBatch fragment;
    private final int sourceRow;
    private final int ordinal;

    private RowCursor(ArrowBatch fragment, int sourceRow, int ordinal) {
      this.fragment = fragment;
      this.sourceRow = sourceRow;
      this.ordinal = ordinal;
    }

    public ArrowBatch fragment() {
      return fragment;
    }

    public int sourceRow() {
      return sourceRow;
    }

    public int ordinal() {
      return ordinal;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      RowCursor that = (RowCursor) other;
      return java.util.Objects.equals(fragment, that.fragment)
          && sourceRow == that.sourceRow
          && ordinal == that.ordinal;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(fragment);
      result = 31 * result + sourceRow;
      result = 31 * result + ordinal;
      return result;
    }

    @Override
    public String toString() {
      return "RowCursor[fragment="
          + fragment
          + ", sourceRow="
          + sourceRow
          + ", ordinal="
          + ordinal
          + "]";
    }
  }

  private static final class PartialParent {
    private final int[] keyGroups;
    private final int expected;
    private final Map<Integer, ArrowBatch> fragments = new HashMap<>();

    private PartialParent(int[] keyGroups, int expected) {
      if (expected <= 0) {
        throw new IllegalArgumentException("Expected fragment count must be positive: " + expected);
      }
      this.keyGroups = keyGroups;
      this.expected = expected;
    }

    private boolean isComplete() {
      return fragments.size() == expected;
    }

    @Override
    public String toString() {
      return "expected=" + expected + ", received=" + fragments.keySet();
    }
  }
}
