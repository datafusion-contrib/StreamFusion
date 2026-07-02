package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar eager (push→emit) deduplication: Arrow in, Arrow out. Serves the three non-buffered dedup
 * variants — rowtime keep-last ({@code RowTimeDeduplicateFunction}), proctime keep-last ({@code
 * ProcTimeDeduplicateKeepLastRowFunction}), and proctime keep-first ({@code
 * ProcTimeDeduplicateKeepFirstRowFunction}). Keep-last keeps the winning row per key and emits a
 * retract changelog eagerly on each input batch ({@code +I} for a key's first row, {@code
 * -U}(previous)/{@code +U}(new) on replacement — the kind rides the {@code $row_kind$} column);
 * keep-first emits each key's first row ({@code +I}, insert-only) and drops the rest. A rowtime order
 * keeps the max-rowtime row; proctime uses arrival order. Insert-only input. Keys are co-located by
 * the columnar shuffle; the per-key stored row and the checkpointed handle state live here. (Rowtime
 * keep-first is watermark-buffered — see {@link NativeColumnarDeduplicateOperator}.)
 */
public class NativeColumnarKeepLastDeduplicateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final boolean generateUpdateBefore;
  private final boolean rowtimeOrdered;
  private final boolean keepFirst;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarKeepLastDeduplicateOperator(
      int[] partitionColumns,
      int rowtimeColumn,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst) {
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.generateUpdateBefore = generateUpdateBefore;
    this.rowtimeOrdered = rowtimeOrdered;
    this.keepFirst = keepFirst;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-keep-last-deduplicate-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    handle =
        snapshot == null
            ? Native.createKeepLastDeduplicator(
                partitionColumns,
                rowtimeColumn,
                generateUpdateBefore,
                rowtimeOrdered,
                keepFirst,
                memoryBudget.bytes())
            : Native.restoreKeepLastDeduplicator(
                partitionColumns,
                rowtimeColumn,
                generateUpdateBefore,
                rowtimeOrdered,
                keepFirst,
                snapshot,
                memoryBudget.bytes());
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.pushKeepLastDeduplicator(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.keepLastDeduplicatorStateBytes(handle));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotKeepLastDeduplicator(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeKeepLastDeduplicator(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
