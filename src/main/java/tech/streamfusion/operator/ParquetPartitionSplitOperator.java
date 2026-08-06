package tech.streamfusion.operator;

import tech.streamfusion.parquet.NativeParquet;
import tech.streamfusion.arrow.ArrowConversion;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.connector.file.table.RowDataPartitionComputer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.PartitionPathUtils;

/**
 * Routes the sink's Arrow batches to filesystem buckets. An unpartitioned table passes each batch
 * through under the root bucket; a partitioned table splits the batch natively into single-key
 * groups (rows cross the boundary in batches that can span partitions, and slicing row-wise on the
 * JVM side would mean a transpose), then names each group's bucket by reading the partition values
 * off its first row through Flink's own partition-path code — so escaping, null handling, and value
 * stringification match the host sink by construction.
 */
public class ParquetPartitionSplitOperator extends AbstractStreamOperator<PartitionedArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, PartitionedArrowBatch> {

  private final RowType rowType;
  private final List<String> partitionKeys;
  private final String defaultPartitionName;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient RowDataPartitionComputer partitionComputer;
  private transient int[] partitionColumns;
  private transient Counter rowsWritten;

  public ParquetPartitionSplitOperator(
      RowType rowType, List<String> partitionKeys, String defaultPartitionName) {
    this.rowType = rowType;
    this.partitionKeys = partitionKeys;
    this.defaultPartitionName = defaultPartitionName;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    DataType[] columnTypes =
        rowType.getChildren().stream()
            .map(TypeConversions::fromLogicalToDataType)
            .toArray(DataType[]::new);
    partitionComputer =
        new RowDataPartitionComputer(
            defaultPartitionName,
            rowType.getFieldNames().toArray(new String[0]),
            columnTypes,
            partitionKeys.toArray(new String[0]));
    partitionColumns =
        partitionKeys.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
    // Comet's native-write surface. The legacy StreamingFileSink writer owns part-file creation and
    // byte output, so those two counters remain zero until that writer exposes a metric context.
    getMetricGroup().counter("files_written");
    getMetricGroup().counter("bytes_written");
    rowsWritten = getMetricGroup().counter("rows_written");
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    int inputRows = element.getValue().rowCount();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), inputRows);
    rowsWritten.inc(inputRows);
    if (partitionKeys.isEmpty()) {
      int rows = element.getValue().rowCount();
      ColumnarRecordMetrics.forward(
          output,
          getMetricGroup(),
          new StreamRecord<>(new PartitionedArrowBatch(element.getValue().root(), "")),
          rows);
      return;
    }

    VectorSchemaRoot batch = element.getValue().root();
    // The batch's buffers belong to the upstream operator's allocator; export with that allocator
    // (buffers associate only within one allocator root).
    BufferAllocator batchAllocator =
        batch.getFieldVectors().isEmpty() ? allocator : batch.getFieldVectors().get(0).getAllocator();
    long split;
    try (ArrowArray array = ArrowArray.allocateNew(batchAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(batchAllocator)) {
      Data.exportVectorSchemaRoot(batchAllocator, batch, dictionaries, array, schema);
      split = NativeParquet.splitByPartitionColumns(
          array.memoryAddress(), schema.memoryAddress(), partitionColumns);
    } finally {
      batch.close();
    }

    try {
      while (true) {
        VectorSchemaRoot group;
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          if (!NativeParquet.nextPartitionSlice(
              split, outArray.memoryAddress(), outSchema.memoryAddress())) {
            break;
          }
          group = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
        }
        RowData firstRow = ArrowConversion.createArrowReader(group, rowType).read(0);
        String bucketId =
            PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(firstRow));
        ColumnarRecordMetrics.forward(
            output,
            getMetricGroup(),
            new StreamRecord<>(new PartitionedArrowBatch(group, bucketId)),
            group.getRowCount());
      }
    } finally {
      NativeParquet.closePartitionSplit(split);
    }
  }
}
