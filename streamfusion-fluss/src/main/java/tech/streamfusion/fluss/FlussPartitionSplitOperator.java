package tech.streamfusion.fluss;

import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.row.RowPartitionGetter;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.*;

/** Native partition splitting with the released Fluss partition-name contract. */
public final class FlussPartitionSplitOperator extends FlinkStreamOperator<PartitionedArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, PartitionedArrowBatch> {
  private final RowType type;
  private final List<String> keys;
  private transient int[] columns;
  private transient RowPartitionGetter getter;

  public FlussPartitionSplitOperator(RowType type, List<String> keys) {
    this.type = type;
    this.keys = List.copyOf(keys);
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    columns = keys.stream().mapToInt(type.getFieldNames()::indexOf).toArray();
    getter = new RowPartitionGetter(FlinkConversions.toFlussRowType(type), keys);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    long split;
    try (VectorSchemaRoot root = element.getValue().root()) {
      var allocator = root.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        split =
            Native.splitByPartitionColumns(array.memoryAddress(), schema.memoryAddress(), columns);
      }
    }
    try {
      while (true) {
        VectorSchemaRoot root;
        try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          if (!Native.nextPartitionSlice(split, array.memoryAddress(), schema.memoryAddress()))
            break;
          root =
              Data.importVectorSchemaRoot(
                  NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
        }
        String partition;
        try {
          partition =
              getter.getPartition(
                  new FlinkAsFlussRow(ArrowConversion.createArrowReader(root, type).read(0)));
        } catch (Throwable failure) {
          root.close();
          throw failure;
        }
        var batch = new PartitionedArrowBatch(root, partition);
        StreamRecord<PartitionedArrowBatch> record =
            element.hasTimestamp()
                ? new StreamRecord<>(batch, element.getTimestamp())
                : new StreamRecord<>(batch);
        ColumnarRecordMetrics.forward(output, getMetricGroup(), record, root.getRowCount());
      }
    } finally {
      Native.closePartitionSplit(split);
    }
  }
}
