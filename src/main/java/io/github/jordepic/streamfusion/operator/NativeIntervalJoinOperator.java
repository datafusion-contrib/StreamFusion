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
 * Columnar event-time INNER interval join (Arrow in on both inputs, Arrow out): the join of
 * {@code a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper}. Each input batch is
 * handed to the native joiner, which buffers it per equi-join key and immediately returns the rows
 * it matches against the other side already buffered — so a pair is emitted once, when the second of
 * its two rows arrives. The buffering, the probe, and the watermark-driven eviction all live
 * natively; this layer moves batches across the bridge and owns the handle's checkpointed state.
 *
 * <p>Flink delivers each input's watermark to {@link #processWatermark1}/{@link #processWatermark2};
 * the base operator combines them into the minimum and calls {@link #processWatermark}, which we
 * override to advance the joiner's eviction frontier before forwarding the watermark downstream.
 */
public class NativeIntervalJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch> {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final long lowerMillis;
  private final long upperMillis;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final EncodedPredicate predicate;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeIntervalJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long lowerMillis,
      long upperMillis,
      int joinType,
      RowType leftType,
      RowType rightType,
      EncodedPredicate predicate) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.lowerMillis = lowerMillis;
    this.upperMillis = upperMillis;
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
                    "streamfusion-interval-join-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    // Hand both sides' Arrow schemas to the joiner up front so an outer join can type the null-padding
    // for a side before that side's first batch arrives.
    BufferAllocator alloc = NativeAllocator.SHARED;
    CDataDictionaryProvider dicts = NativeAllocator.DICTIONARIES;
    try (ArrowSchema leftSchema = ArrowSchema.allocateNew(alloc);
        ArrowSchema rightSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(leftType), dicts, leftSchema);
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(rightType), dicts, rightSchema);
      handle =
          snapshot == null
              ? Native.createIntervalJoiner(
                  leftKeys,
                  rightKeys,
                  leftTime,
                  rightTime,
                  lowerMillis,
                  upperMillis,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.longs,
                  predicate.doubles,
                  predicate.strings)
              : Native.restoreIntervalJoiner(
                  leftKeys,
                  rightKeys,
                  leftTime,
                  rightTime,
                  lowerMillis,
                  upperMillis,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.longs,
                  predicate.doubles,
                  predicate.strings,
                  snapshot);
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
    join(element.getValue(), true);
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    join(element.getValue(), false);
  }

  /** Pushes a batch to its side of the joiner and emits the matched pairs it returns (if any). */
  private void join(ArrowBatch batch, boolean left) {
    VectorSchemaRoot in = batch.root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (left) {
        Native.pushLeftIntervalJoiner(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.pushRightIntervalJoiner(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no matches for this batch
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // Eviction may produce null-padded rows for unmatched outer rows whose interval just closed; emit
    // them before forwarding the watermark.
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.advanceIntervalJoiner(
          handle, mark.getTimestamp(), outArray.memoryAddress(), outSchema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    }
    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotIntervalJoiner(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeIntervalJoiner(handle);
      handle = 0;
    }
    super.close();
  }
}
