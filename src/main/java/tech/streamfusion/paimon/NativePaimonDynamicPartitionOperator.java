package tech.streamfusion.paimon;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.partition.DataStatisticsOperator;
import org.apache.paimon.flink.sink.partition.DataStatisticsOperatorFactory;
import org.apache.paimon.flink.sink.partition.StatisticsOrRecord;
import org.apache.paimon.flink.sink.partition.StatisticsOrRecordChannelComputer;
import org.apache.paimon.schema.TableSchema;
import tech.streamfusion.Native;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/** Paimon's adaptive partition shuffle over Arrow payloads and partition-only metadata. */
public final class NativePaimonDynamicPartitionOperator
    extends AbstractStreamOperator<BucketedArrowBatch>
    implements OneInputStreamOperator<BucketedArrowBatch, BucketedArrowBatch> {

  private final int partitionArity;
  private final int numChannels;
  private final DataStatisticsOperator statistics;
  private final StatisticsOrRecordChannelComputer channels;
  private int selectedChannel;

  NativePaimonDynamicPartitionOperator(
      StreamOperatorParameters<BucketedArrowBatch> parameters,
      TableSchema partitionSchema,
      int numChannels) {
    setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    this.partitionArity = partitionSchema.partitionKeys().size();
    this.numChannels = numChannels;
    this.channels = new StatisticsOrRecordChannelComputer(partitionSchema);
    this.statistics =
        new DataStatisticsOperatorFactory(partitionSchema)
            .createStreamOperator(
                new StreamOperatorParameters<>(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    new StatisticsOutput(),
                    parameters::getProcessingTimeService,
                    parameters.getOperatorEventDispatcher(),
                    parameters.getMailboxExecutor()));
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    // The released statistics operator has no backend state: its checkpoint hook sends counts
    // to the stock coordinator and resets them, and recovery starts collecting fresh counts.
    statistics.initializeState(context);
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    channels.setup(numChannels);
    statistics.open();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    statistics.snapshotState(context);
  }

  @Override
  public void processElement(StreamRecord<BucketedArrowBatch> element) throws Exception {
    BucketedArrowBatch batch = element.getValue();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), batch.rowCount());
    long route;
    try (VectorSchemaRoot root = batch.root()) {
      StreamRecord<InternalRow> partition =
          new StreamRecord<>(PaimonPartitions.fromBytes(batch.partition(), partitionArity));
      int[] destinations = new int[root.getRowCount()];
      // Preserve Paimon's per-row counting and weighted random selection. Only the already
      // encoded partition key enters Java; payload columns stay in their Arrow buffers.
      for (int row = 0; row < destinations.length; row++) {
        statistics.processElement(partition);
        destinations[row] = selectedChannel;
      }
      BufferAllocator allocator = root.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        route =
            Native.routeByAssignedBuckets(
                array.memoryAddress(), schema.memoryAddress(), destinations);
      }
    }
    try {
      while (true) {
        try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          int channel =
              Native.nextBucketRoute(route, array.memoryAddress(), schema.memoryAddress());
          if (channel < 0) {
            break;
          }
          VectorSchemaRoot root =
              Data.importVectorSchemaRoot(
                  NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
          ColumnarRecordMetrics.forward(
              output,
              getMetricGroup(),
              new StreamRecord<>(new BucketedArrowBatch(root, batch.partition(), channel)),
              root.getRowCount());
        }
      }
    } finally {
      Native.closeBucketRoute(route);
    }
  }

  private final class StatisticsOutput implements Output<StreamRecord<StatisticsOrRecord>> {
    @Override
    public void collect(StreamRecord<StatisticsOrRecord> record) {
      // Statistics events replace the stock assignment map. Their arbitrary output channel is
      // discarded, as it is by Paimon's downstream Strip Statistics operator.
      selectedChannel = channels.channel(record.getValue());
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      output.emitWatermark(watermark);
    }

    @Override
    public void emitWatermark(WatermarkEvent watermark) {
      output.emitWatermark(watermark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus status) {
      output.emitWatermarkStatus(status);
    }

    @Override
    public <X> void collect(OutputTag<X> tag, StreamRecord<X> record) {
      output.collect(tag, record);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker marker) {
      output.emitLatencyMarker(marker);
    }

    @Override
    public void emitRecordAttributes(RecordAttributes attributes) {
      output.emitRecordAttributes(attributes);
    }

    @Override
    public void close() {}
  }

  public static final class Factory extends AbstractStreamOperatorFactory<BucketedArrowBatch>
      implements OneInputStreamOperatorFactory<BucketedArrowBatch, BucketedArrowBatch>,
          CoordinatedOperatorFactory<BucketedArrowBatch> {
    private final TableSchema partitionSchema;
    private final int numChannels;

    public Factory(TableSchema schema, int numChannels) {
      this.partitionSchema = schema.project(schema.partitionKeys());
      this.numChannels = numChannels;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String name, OperatorID id) {
      return new DataStatisticsOperatorFactory(partitionSchema).getCoordinatorProvider(name, id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<BucketedArrowBatch>> T createStreamOperator(
        StreamOperatorParameters<BucketedArrowBatch> parameters) {
      return (T) new NativePaimonDynamicPartitionOperator(parameters, partitionSchema, numChannels);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return NativePaimonDynamicPartitionOperator.class;
    }
  }
}
