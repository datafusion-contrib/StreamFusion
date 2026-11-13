package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.batch.BoundedArrowOutput;

/** DataFusion's bounded hash join, with input demand driven by Flink's batch task. */
public final class NativeBatchJoinOperator extends FlinkStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch>,
        BoundedMultiInput,
        InputSelectable {
  private final String[] schemas;
  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int kind;
  private final boolean buildLeft;
  private final boolean nullEquals;
  private final int batchRows;
  private final boolean nestedLoop;
  private final boolean mergeJoin;
  private final EncodedPredicate predicate;
  private transient long handle;
  private transient Schema resultSchema;
  private transient Schema[] inputSchemas;
  private transient int needed;
  private transient int ended;
  private transient boolean complete;
  private transient long[] stats;

  public NativeBatchJoinOperator(
      RowType left,
      RowType right,
      RowType result,
      int[] leftKeys,
      int[] rightKeys,
      int kind,
      boolean buildLeft,
      boolean nullEquals,
      int batchRows,
      boolean nestedLoop,
      EncodedPredicate predicate,
      boolean mergeJoin) {
    schemas =
        new String[] {
          ArrowConversion.toArrowInputSchema(left, false).toJson(),
          ArrowConversion.toArrowInputSchema(right, false).toJson(),
          ArrowConversion.toArrowInputSchema(result, false).toJson()
        };
    this.leftKeys = leftKeys.clone();
    this.rightKeys = rightKeys.clone();
    this.kind = kind;
    this.buildLeft = buildLeft;
    this.nullEquals = nullEquals;
    this.batchRows = batchRows;
    this.nestedLoop = nestedLoop;
    this.mergeJoin = mergeJoin;
    this.predicate = predicate;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    stats = new long[3];
    getMetricGroup().gauge("batchJoinReservedBytes", () -> stats[2]);
    resultSchema = Schema.fromJSON(schemas[2]);
    inputSchemas = new Schema[] {Schema.fromJSON(schemas[0]), Schema.fromJSON(schemas[1])};
    try (var left = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var right = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var result = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(NativeAllocator.SHARED, Schema.fromJSON(schemas[0]), null, left);
      Data.exportSchema(NativeAllocator.SHARED, Schema.fromJSON(schemas[1]), null, right);
      Data.exportSchema(NativeAllocator.SHARED, resultSchema, null, result);
      long filter =
          predicate.kinds.length == 0
              ? 0
              : Native.createFilterExpression(
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.longs,
                  predicate.doubles,
                  predicate.strings);
      try {
        if (mergeJoin) {
          handle =
              Native.createBoundedMergeJoin(
                  left.memoryAddress(),
                  right.memoryAddress(),
                  result.memoryAddress(),
                  leftKeys,
                  rightKeys,
                  kind,
                  nullEquals,
                  filter,
                  getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths(),
                  batchRows,
                  tech.streamfusion.compat.RuntimeCompat.taskName(getRuntimeContext()));
        } else
          handle =
              Native.createBoundedJoinPlan(
                  left.memoryAddress(),
                  right.memoryAddress(),
                  result.memoryAddress(),
                  leftKeys,
                  rightKeys,
                  kind,
                  buildLeft,
                  nullEquals,
                  nestedLoop,
                  filter,
                  getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths(),
                  batchRows,
                  tech.streamfusion.compat.RuntimeCompat.taskName(getRuntimeContext()));
      } finally {
        if (filter != 0) Native.closeFilterExpression(filter);
        left.release();
        right.release();
        result.release();
      }
    }
    drain();
  }

  private void accept(int port, StreamRecord<ArrowBatch> record) {
    var root = record.getValue().root();
    try {
      if ((ended & (1 << port)) != 0)
        throw new IllegalStateException("bounded join input already ended");
      var expected = inputSchemas[port].getFields();
      var actual = root.getSchema().getFields();
      if (expected.size() != actual.size())
        throw new IllegalStateException("bounded join input width changed");
      for (int column = 0; column < expected.size(); column++) {
        if (!expected.get(column).getType().equals(actual.get(column).getType())
            || actual.get(column).getDictionary() != null)
          throw new IllegalStateException("bounded join input representation changed");
      }
      if (!complete) {
        try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
          Data.exportVectorSchemaRoot(
              NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array);
          try {
            Native.pushBoundedTaskInput(handle, port, array.memoryAddress());
          } finally {
            array.release();
          }
        }
      }
      ColumnarRecordMetrics.countIngested(getMetricGroup(), root.getRowCount());
    } finally {
      root.close();
    }
    if (!complete) drain();
  }

  private void drain() {
    while (true) {
      if (Thread.currentThread().isInterrupted())
        throw new java.util.concurrent.CancellationException("bounded join interrupted");
      try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
        int status = Native.nextBoundedSorter(handle, array.memoryAddress(), 0);
        stats = Native.boundedSorterExecutionStats(handle);
        if (status == 2) continue;
        if (status == -1) {
          complete = true;
          needed = 3 & ~ended;
          long closing = handle;
          handle = 0;
          Native.closeBoundedSorter(closing);
          return;
        }
        if (status == 0) {
          needed = Native.boundedTaskNeededInputs(handle);
          if ((needed & ~ended) == 0)
            throw new IllegalStateException("bounded join requested no live input");
          return;
        }
        ColumnarRecordMetrics.emit(
            output, getMetricGroup(), BoundedArrowOutput.importBatch(handle, array, resultSchema));
      }
    }
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> record) {
    accept(0, record);
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> record) {
    accept(1, record);
  }

  @Override
  public void processWatermark1(Watermark mark) {}

  @Override
  public void processWatermark2(Watermark mark) {}

  @Override
  public void endInput(int inputId) {
    int port = inputId - 1;
    ended |= 1 << port;
    if (!complete) {
      Native.finishBoundedTaskInput(handle, port);
      drain();
    } else {
      needed = 3 & ~ended;
    }
  }

  @Override
  public InputSelection nextSelection() {
    if (needed == 0) return InputSelection.ALL;
    var selection = new InputSelection.Builder();
    if ((needed & 1) != 0) selection.select(1);
    if ((needed & 2) != 0) selection.select(2);
    return selection.build();
  }

  @Override
  public void close() throws Exception {
    try {
      if (handle != 0) {
        long closing = handle;
        handle = 0;
        Native.closeBoundedSorter(closing);
      }
    } finally {
      super.close();
    }
  }
}
