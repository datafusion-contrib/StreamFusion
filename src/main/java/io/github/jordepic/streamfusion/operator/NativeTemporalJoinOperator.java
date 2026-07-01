package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar event-time temporal table join (Arrow in on both inputs, Arrow out):
 * {@code probe JOIN versioned FOR SYSTEM_TIME AS OF probe.rowtime ON probe.k = versioned.k}. The
 * probe (left) input is a regular stream; the build (right) input is a versioned changelog (an
 * upsert/retract stream keyed by the equi-join key). Both inputs are buffered natively; on a watermark
 * the native joiner emits, for each buffered probe row the watermark has passed, the join with the
 * build version valid at the probe row's time — a buffer-then-emit-on-watermark flow like the window
 * join, but resolving a versioned lookup rather than a window match. The versioned state, the
 * per-probe-row lookup, and the state cleanup live natively; this layer moves batches across the
 * bridge and owns the handle's checkpointed state.
 *
 * <p>Flink delivers each input's watermark to {@code processWatermark1}/{@code processWatermark2}; the
 * base operator combines them into the minimum and calls {@link #processWatermark}, which advances the
 * joiner (emitting the now-resolvable probe rows) before forwarding the watermark downstream. Output
 * carries the changelog kind on the {@code $row_kind$} column (the probe row's kind, as Flink does).
 *
 * <p>Only INNER and LEFT are possible (Flink rejects RIGHT/FULL for a temporal join), so only the
 * build side can be absent; a LEFT join null-pads a probe row that finds no valid version. Processing
 * time is intentionally unsupported — Flink itself rejects a processing-time temporal table join.
 */
public class NativeTemporalJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch> {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final EncodedPredicate predicate;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeTemporalJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      int joinType,
      RowType leftType,
      RowType rightType,
      EncodedPredicate predicate) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-temporal-join-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    // Hand both sides' Arrow schemas to the joiner up front so a LEFT join can type the null-padding
    // for the build side before that side's first batch arrives.
    BufferAllocator alloc = NativeAllocator.SHARED;
    CDataDictionaryProvider dicts = NativeAllocator.DICTIONARIES;
    try (ArrowSchema leftSchema = ArrowSchema.allocateNew(alloc);
        ArrowSchema rightSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(leftType), dicts, leftSchema);
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(rightType), dicts, rightSchema);
      predicate.bind();
      memoryBudget = ManagedMemoryBudget.reserveFor(this);
      handle =
          snapshot == null
              ? Native.createTemporalJoiner(
                  leftKeys,
                  rightKeys,
                  leftTime,
                  rightTime,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.boundLongs(),
                  predicate.doubles,
                  predicate.strings,
                  memoryBudget.bytes())
              : Native.restoreTemporalJoiner(
                  leftKeys,
                  rightKeys,
                  leftTime,
                  rightTime,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.boundLongs(),
                  predicate.doubles,
                  predicate.strings,
                  snapshot,
                  memoryBudget.bytes());
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    buffer(element.getValue(), true);
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    buffer(element.getValue(), false);
  }

  /** Hands a batch to its side of the joiner, which buffers it (no output until a watermark). */
  private void buffer(ArrowBatch batch, boolean left) {
    VectorSchemaRoot in = batch.root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      if (left) {
        Native.pushLeftTemporalJoiner(handle, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.pushRightTemporalJoiner(handle, array.memoryAddress(), schema.memoryAddress());
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    advance(mark.getTimestamp());
    super.processWatermark(mark);
  }

  @Override
  public void finish() throws Exception {
    advance(Long.MAX_VALUE); // end of input: resolve every remaining buffered probe row
    super.finish();
  }

  /** Advances the watermark, emitting the joined rows for buffered probe rows it has passed. */
  private void advance(long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.advanceTemporalJoiner(handle, watermark, array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no probe rows resolvable at this watermark
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotTemporalJoiner(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeTemporalJoiner(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    predicate.unbind();
    super.close();
  }
}
