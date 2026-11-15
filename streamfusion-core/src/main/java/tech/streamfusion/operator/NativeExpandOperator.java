package tech.streamfusion.operator;

import java.io.Serializable;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Evaluates each grouping-set projection over the same borrowed Arrow input, as Comet Expand does.
 */
public final class NativeExpandOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  public static final class Projection implements Serializable {
    public final int[] kinds, payload, childCounts, roots;
    public final long[] longs;
    public final double[] doubles;
    public final String[] strings, names;

    public Projection(
        int[] kinds,
        int[] payload,
        int[] childCounts,
        int[] roots,
        long[] longs,
        double[] doubles,
        String[] strings,
        String[] names) {
      this.kinds = kinds;
      this.payload = payload;
      this.childCounts = childCounts;
      this.roots = roots;
      this.longs = longs;
      this.doubles = doubles;
      this.strings = strings;
      this.names = names;
    }
  }

  private final List<Projection> projections;
  private transient long[] handles;

  public NativeExpandOperator(List<Projection> projections) {
    this.projections = projections;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    handles = new long[projections.size()];
    try {
      for (int i = 0; i < handles.length; i++) {
        var p = projections.get(i);
        handles[i] =
            Native.createCalcExpression(
                p.kinds,
                p.payload,
                p.childCounts,
                p.longs,
                p.doubles,
                p.strings,
                p.roots,
                -1,
                p.names);
      }
    } catch (Throwable failure) {
      release();
      throw failure;
    }
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    var input = element.getValue();
    var root = input.root();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), root.getRowCount());
    var allocator = NativeAllocator.SHARED;
    var inputAllocator =
        root.getFieldVectors().isEmpty() ? allocator : root.getFieldVectors().get(0).getAllocator();
    try {
      for (long handle : handles) {
        VectorSchemaRoot result;
        try (var in = ArrowArray.allocateNew(inputAllocator);
            var schema = ArrowSchema.allocateNew(inputAllocator);
            var out = ArrowArray.allocateNew(allocator);
            var outSchema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(
              inputAllocator, root, NativeAllocator.DICTIONARIES, in, schema);
          Native.calcExpression(
              handle,
              in.memoryAddress(),
              schema.memoryAddress(),
              out.memoryAddress(),
              outSchema.memoryAddress());
          result =
              Data.importVectorSchemaRoot(allocator, out, outSchema, NativeAllocator.DICTIONARIES);
        }
        ColumnarRecordMetrics.emit(
            output, getMetricGroup(), new ArrowBatch(result, input.keyGroup()));
      }
    } finally {
      root.close();
    }
  }

  private void release() {
    if (handles != null) {
      for (long handle : handles) if (handle != 0) Native.closeCalcExpression(handle);
      handles = null;
    }
  }

  @Override
  public void close() throws Exception {
    try {
      release();
    } finally {
      super.close();
    }
  }
}
