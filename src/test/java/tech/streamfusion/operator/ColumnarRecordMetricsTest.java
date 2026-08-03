package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

/**
 * The runtime charges one record per collect, so a columnar operator's counters would report
 * batches. These pin the correction: the counter ends up on rows, and measuring a batch never
 * takes it.
 */
class ColumnarRecordMetricsTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"k"});

  private static RowData row(long k) {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, k);
    return row;
  }

  private static ArrowBatch batchOf(BufferAllocator allocator, int rows) {
    List<RowData> data = new ArrayList<>();
    for (int i = 0; i < rows; i++) {
      data.add(row(i));
    }
    return new ArrowBatch(RowDataArrowConverter.write(data, SCHEMA, allocator));
  }

  /** Stands in for the runtime's own counting output, which charges exactly one per collect. */
  private static final class CountingCollector implements Output<StreamRecord<ArrowBatch>> {
    private final OperatorMetricGroup metrics;
    private final List<ArrowBatch> collected = new ArrayList<>();

    CountingCollector(OperatorMetricGroup metrics) {
      this.metrics = metrics;
    }

    @Override
    public void collect(StreamRecord<ArrowBatch> record) {
      metrics.getIOMetricGroup().getNumRecordsOutCounter().inc();
      collected.add(record.getValue());
    }

    @Override
    public <X> void collect(OutputTag<X> tag, StreamRecord<X> record) {}

    @Override
    public void emitWatermark(Watermark mark) {}

    @Override
    public void emitWatermarkStatus(WatermarkStatus status) {}

    @Override
    public void emitLatencyMarker(LatencyMarker marker) {}

    @Override
    public void emitWatermark(WatermarkEvent watermark) {}

    @Override
    public void emitRecordAttributes(RecordAttributes attributes) {}

    @Override
    public void close() {}
  }

  @Test
  void emittingABatchChargesItsRowsRatherThanOneRecord() {
    OperatorMetricGroup metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
    CountingCollector output = new CountingCollector(metrics);
    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowBatch batch = batchOf(allocator, 4096);
      ColumnarRecordMetrics.emit(output, metrics, batch);

      assertEquals(
          4096, metrics.getIOMetricGroup().getNumRecordsOutCounter().getCount(), "rows, not batches");
      assertEquals(1, output.collected.size(), "the batch is emitted exactly once");
      try (VectorSchemaRoot root = output.collected.get(0).root()) {
        assertEquals(4096, root.getRowCount(), "the consumer still gets to take the batch");
      }
    }
  }

  /**
   * Measuring must not take the batch. {@code root()} is a hand-off — under a share it spends a
   * consumer and returns that consumer's retained view — so counting through it leaks the view
   * nobody closes and starves a real reader. The allocator balance is the assertion that matters.
   */
  @Test
  void measuringASharedBatchLeavesEveryConsumersShareIntact() {
    OperatorMetricGroup metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowBatch batch = batchOf(allocator, 8);
      batch.shareAcross(2);

      // What the metrics path does: measure, repeatedly, without consuming.
      assertEquals(8, batch.rowCount());
      assertEquals(8, batch.rowCount());
      ColumnarRecordMetrics.countIngested(metrics, batch.rowCount());

      // Both declared consumers can still take their share, and closing them frees everything.
      try (VectorSchemaRoot first = batch.root();
          VectorSchemaRoot second = batch.root()) {
        assertEquals(8, first.getRowCount());
        assertEquals(8, second.getRowCount());
      }
      assertEquals(0, allocator.getAllocatedMemory(), "no view left unclosed");
    }
  }

  @Test
  void anEmptyBatchIsLeftAtTheRuntimesOwnSingleCount() {
    OperatorMetricGroup metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
    CountingCollector output = new CountingCollector(metrics);
    try (BufferAllocator allocator = new RootAllocator()) {
      ColumnarRecordMetrics.emit(output, metrics, batchOf(allocator, 0));
      assertEquals(
          1,
          metrics.getIOMetricGroup().getNumRecordsOutCounter().getCount(),
          "an empty batch must not drive the counter backwards");
      output.collected.get(0).root().close();
    }
  }
}
