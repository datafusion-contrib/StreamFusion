package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.partition.DataStatisticsOperatorFactory;
import org.apache.paimon.flink.sink.partition.StatisticsOrRecord;
import org.apache.paimon.flink.sink.partition.StatisticsOrRecordChannelComputer;
import org.apache.paimon.flink.sink.partition.StatisticsOrRecordTypeInfo;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.BucketedArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonDynamicPartitionOperatorTest {
  private static final TableSchema SCHEMA =
      new TableSchema(
          0,
          org.apache.paimon.types.RowType.builder()
              .field("id", DataTypes.INT())
              .field("pt", DataTypes.STRING())
              .build()
              .getFields(),
          1,
          List.of("pt"),
          List.of(),
          Map.of("bucket", "-1"),
          null);
  private static final RowType ROW_TYPE =
      RowType.of(new LogicalType[] {new IntType(), new VarCharType()}, new String[] {"id", "pt"});

  @Test
  void checkpointCountsMatchStockAndRecoveryStartsFresh() throws Exception {
    var nativeFactory = nativeFactory(3);
    var stockFactory = stockFactory();
    OperatorSubtaskState checkpoint;
    try (var ours = new OneInputStreamOperatorTestHarness<>(nativeFactory);
        var stock = new OneInputStreamOperatorTestHarness<>(stockFactory)) {
      ours.setup(new BucketedArrowBatchSerializer());
      setupStock(stock);
      ours.open();
      stock.open();
      feed(ours, "a", 17);
      feed(ours, null, 11);
      for (int i = 0; i < 17; i++) stock.processElement(new StreamRecord<>(row("a")));
      for (int i = 0; i < 11; i++) stock.processElement(new StreamRecord<>(row(null)));
      checkpoint = ours.snapshot(1, 0);
      stock.snapshot(1, 0);
      assertEquals(decode(stockFactory.events.get(0)), decode(nativeFactory.events.get(0)));
      assertEquals(
          Map.of(partition("a"), 17L, partition(null), 11L), decode(nativeFactory.events.get(0)));
      ours.snapshot(2, 0);
      assertTrue(decode(nativeFactory.events.get(1)).isEmpty());
      feed(ours, "uncheckpointed", 3);
      closeBatches(ours);
    }
    var restoredFactory = nativeFactory(3);
    try (var restored = new OneInputStreamOperatorTestHarness<>(restoredFactory)) {
      restored.setup(new BucketedArrowBatchSerializer());
      restored.initializeState(checkpoint);
      restored.open();
      feed(restored, "after-restore", 5);
      restored.snapshot(3, 0);
      assertEquals(Map.of(partition("after-restore"), 5L), decode(restoredFactory.events.get(0)));
      closeBatches(restored);
    }
  }

  @Test
  void globalStatisticsReplaceRoutingAndWatermarksPassThrough() throws Exception {
    var factory = nativeFactory(7);
    try (var harness = new OneInputStreamOperatorTestHarness<>(factory)) {
      harness.setup(new BucketedArrowBatchSerializer());
      harness.open();
      feed(harness, "a", 2048);
      Set<Integer> unknownChannels = consume(harness, 2048);
      int start = ChannelComputer.startChannel(partition("a"), 7);
      assertEquals(
          IntStream.range(0, 4)
              .map(i -> (start + i) % 7)
              .boxed()
              .collect(java.util.stream.Collectors.toSet()),
          unknownChannels);

      // A single known partition has weight on every writer after global statistics arrive.
      OperatorEvent global = event(Map.of("a", 70));
      factory.handler.handleOperatorEvent(global);
      feed(harness, "a", 2048);
      assertEquals(
          IntStream.range(0, 7).boxed().collect(java.util.stream.Collectors.toSet()),
          consume(harness, 2048));

      // Seven equally sized partitions each belong to exactly one writer. Compare the exact
      // channels with Paimon's stock computer, which projects pt from the full input row.
      Map<String, Integer> counts = new HashMap<>();
      for (int i = 0; i < 7; i++) counts.put("p" + i, 10);
      global = event(counts);
      factory.handler.handleOperatorEvent(global);
      StatisticsOrRecordChannelComputer stock = new StatisticsOrRecordChannelComputer(SCHEMA);
      stock.setup(7);
      stock.channel(
          StatisticsOrRecord.fromStatistics(
              new org.apache.paimon.flink.sink.partition.DataStatistics(decode(global))));
      for (String pt : counts.keySet()) {
        feed(harness, pt, 10);
        assertEquals(
            Set.of(stock.channel(StatisticsOrRecord.fromRecord(row(pt)))), consume(harness, 10));
      }
      harness.processWatermark(new Watermark(123));
      harness.processWatermarkStatus(WatermarkStatus.IDLE);
      assertEquals(
          List.of(new Watermark(123), WatermarkStatus.IDLE), new ArrayList<>(harness.getOutput()));
    }
  }

  private static InternalRow row(String pt) {
    return GenericRow.of(0, pt == null ? null : BinaryString.fromString(pt));
  }

  private static BinaryRow partition(String pt) {
    return new RowPartitionKeyExtractor(SCHEMA).partition(row(pt)).copy();
  }

  private static void feed(
      OneInputStreamOperatorTestHarness<BucketedArrowBatch, BucketedArrowBatch> harness,
      String pt,
      int count)
      throws Exception {
    try (var allocator = new RootAllocator()) {
      List<org.apache.flink.table.data.RowData> rows = new ArrayList<>();
      for (int i = 0; i < count; i++)
        rows.add(GenericRowData.of(i, pt == null ? null : StringData.fromString(pt)));
      harness.processElement(
          new StreamRecord<>(
              new BucketedArrowBatch(
                  RowDataArrowConverter.write(rows, ROW_TYPE, allocator),
                  partition(pt).toBytes(),
                  0)));
    }
  }

  private static Set<Integer> consume(
      OneInputStreamOperatorTestHarness<BucketedArrowBatch, BucketedArrowBatch> harness,
      int count) {
    Set<Integer> channels = new HashSet<>();
    Set<Integer> ids = new HashSet<>();
    for (Object output : harness.getOutput()) {
      if (!(output instanceof StreamRecord)) continue;
      BucketedArrowBatch batch = (BucketedArrowBatch) ((StreamRecord<?>) output).getValue();
      channels.add(batch.bucket());
      try (VectorSchemaRoot root = batch.root()) {
        IntVector vector = (IntVector) root.getVector("id");
        int previous = -1;
        for (int i = 0; i < root.getRowCount(); i++) {
          int id = vector.get(i);
          assertTrue(id > previous, "routing must preserve order within each channel");
          assertTrue(ids.add(id), "duplicate payload row");
          previous = id;
        }
      }
    }
    assertEquals(
        IntStream.range(0, count).boxed().collect(java.util.stream.Collectors.toSet()), ids);
    harness.getOutput().clear();
    return channels;
  }

  private static void closeBatches(
      OneInputStreamOperatorTestHarness<BucketedArrowBatch, BucketedArrowBatch> harness) {
    for (Object value : harness.getOutput()) {
      if (value instanceof StreamRecord)
        ((BucketedArrowBatch) ((StreamRecord<?>) value).getValue()).root().close();
    }
    harness.getOutput().clear();
  }

  private static RecordingFactory<BucketedArrowBatch, BucketedArrowBatch> nativeFactory(
      int writers) {
    return new RecordingFactory<>(
        new NativePaimonDynamicPartitionOperator.Factory(SCHEMA, writers));
  }

  private static RecordingFactory<InternalRow, StatisticsOrRecord> stockFactory() {
    return new RecordingFactory<>(new DataStatisticsOperatorFactory(SCHEMA));
  }

  private static OperatorEvent event(Map<String, Integer> counts) throws Exception {
    var factory = stockFactory();
    try (var harness = new OneInputStreamOperatorTestHarness<>(factory)) {
      setupStock(harness);
      harness.open();
      for (var entry : counts.entrySet()) {
        for (int i = 0; i < entry.getValue(); i++)
          harness.processElement(new StreamRecord<>(row(entry.getKey())));
      }
      harness.snapshot(1, 0);
      return factory.events.get(0);
    }
  }

  private static Map<BinaryRow, Long> decode(OperatorEvent event) throws Exception {
    var factory = stockFactory();
    try (var harness = new OneInputStreamOperatorTestHarness<>(factory)) {
      setupStock(harness);
      harness.open();
      factory.handler.handleOperatorEvent(event);
      StatisticsOrRecord output =
          (StatisticsOrRecord) ((StreamRecord<?>) harness.getOutput().remove()).getValue();
      return output.statistics().result();
    }
  }

  private static void setupStock(
      OneInputStreamOperatorTestHarness<InternalRow, StatisticsOrRecord> harness) {
    harness.setup(
        new StatisticsOrRecordTypeInfo(SCHEMA.logicalRowType())
            .createSerializer(new org.apache.flink.api.common.ExecutionConfig()));
  }

  private static final class RecordingFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
      implements OneInputStreamOperatorFactory<IN, OUT> {
    private final OneInputStreamOperatorFactory<IN, OUT> delegate;
    private final List<OperatorEvent> events = new ArrayList<>();
    private OperatorEventHandler handler;

    RecordingFactory(OneInputStreamOperatorFactory<IN, OUT> delegate) {
      this.delegate = delegate;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
        StreamOperatorParameters<OUT> parameters) {
      return delegate.createStreamOperator(
          new StreamOperatorParameters<>(
              parameters.getContainingTask(),
              parameters.getStreamConfig(),
              parameters.getOutput(),
              parameters::getProcessingTimeService,
              new OperatorEventDispatcher() {
                @Override
                public void registerEventHandler(OperatorID id, OperatorEventHandler registered) {
                  handler = registered;
                }

                @Override
                public OperatorEventGateway getOperatorEventGateway(OperatorID id) {
                  return events::add;
                }
              },
              parameters.getMailboxExecutor()));
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader loader) {
      return delegate.getStreamOperatorClass(loader);
    }
  }
}
