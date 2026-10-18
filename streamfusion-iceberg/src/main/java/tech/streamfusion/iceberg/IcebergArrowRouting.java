package tech.streamfusion.iceberg;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/** Sends Java-selected row positions through the shared native batch splitter. */
final class IcebergArrowRouting {
  private IcebergArrowRouting() {}

  static long capture(VectorSchemaRoot root, int[] destinations) {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
        return NativeIceberg.route(array.memoryAddress(), schema.memoryAddress(), destinations);
      } finally {
        if (array.snapshot().release != 0) array.release();
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  static void emit(
      long route,
      StreamRecord<ArrowBatch> input,
      Output<StreamRecord<ArrowBatch>> output,
      OperatorMetricGroup metrics) {
    try {
      while (true) {
        try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          try {
            int channel =
                NativeIceberg.nextRoute(route, array.memoryAddress(), schema.memoryAddress());
            if (channel < 0) break;
            var root =
                Data.importVectorSchemaRoot(
                    NativeAllocator.SHARED,
                    ArrowArray.wrap(array.memoryAddress()),
                    ArrowSchema.wrap(schema.memoryAddress()),
                    NativeAllocator.DICTIONARIES);
            ColumnarRecordMetrics.forward(
                output, metrics, input.copy(new ArrowBatch(root, channel)), root.getRowCount());
          } finally {
            if (array.snapshot().release != 0) array.release();
            if (schema.snapshot().release != 0) schema.release();
          }
        }
      }
    } finally {
      NativeIceberg.closeRoute(route);
    }
  }
}
