package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class SplitByKeyGroupOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});
  private static final RowType NESTED_SCHEMA =
      RowType.of(
          new LogicalType[] {
            new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new ArrayType(new TimestampType(9))),
            new IntType()
          },
          new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  @SuppressWarnings("unchecked")
  void splitsABatchIntoKeyGroupTaggedSubBatches() throws Exception {
    int channels = 4;
    int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(channels);
    int n = 500;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, maxParallelism),
                new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      List<RowData> rows = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        rows.add(row(i % 53, i));
      }
      VectorSchemaRoot in = RowDataArrowConverter.write(rows, SCHEMA, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(in)));

      int total = 0;
      Map<Long, Integer> keyToGroup = new HashMap<>();
      RowDataSerializer serializer = new RowDataSerializer(RowType.of(new BigIntType()));
      for (Object record : harness.getOutput()) {
        if (!(record instanceof StreamRecord)) {
          continue;
        }
        ArrowBatch batch = ((StreamRecord<ArrowBatch>) record).getValue();
        int taggedGroup = batch.keyGroup();
        assertTrue(taggedGroup >= 0 && taggedGroup < maxParallelism, "key group in range");
        try (VectorSchemaRoot sub = batch.root()) {
          for (RowData r : RowDataArrowConverter.read(sub, SCHEMA)) {
            long key = r.getLong(0);
            Integer prev = keyToGroup.put(key, taggedGroup);
            if (prev != null) {
              assertEquals(prev.intValue(), taggedGroup, "key " + key + " split across groups");
            }
            GenericRowData projectedKey = new GenericRowData(1);
            projectedKey.setField(0, key);
            int keyGroup =
                KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                    serializer.toBinaryRow(projectedKey).hashCode(), maxParallelism);
            assertEquals(keyGroup, taggedGroup, "Flink key group for key " + key);
            total++;
          }
        }
      }
      assertEquals(n, total, "all rows preserved");
    }
  }

  @Test
  void partitionerReroutesAKeyGroupAtRestoredParallelism() {
    int maxParallelism = 128;
    int keyGroup = 70;
    ColumnarKeyGroupPartitioner partitioner = new ColumnarKeyGroupPartitioner(maxParallelism);
    partitioner.setup(4);
    StreamPartitioner<ArrowBatch> p = partitioner;
    SerializationDelegate<StreamRecord<ArrowBatch>> delegate = new SerializationDelegate<>(null);
    delegate.setInstance(new StreamRecord<>(new ArrowBatch(null, keyGroup)));
    assertEquals(
        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 4, keyGroup),
        p.selectChannel(delegate));

    // Recovery configures the copied partitioner for the restored topology and filters/reroutes
    // this whole record without inspecting its Arrow rows.
    ColumnarKeyGroupPartitioner restored =
        (ColumnarKeyGroupPartitioner) partitioner.copy();
    restored.configure(maxParallelism);
    restored.setup(7);
    assertEquals(
        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 7, keyGroup),
        restored.selectChannel(delegate));

    delegate.setInstance(new StreamRecord<>(new ArrowBatch(null, -1)));
    assertEquals(0, p.selectChannel(delegate), "an unrouted batch goes to channel 0");
  }

  @Test
  void partitionerUsesKeyGroupRangeRecoveryAndSupportsUnalignedChannelState() {
    ColumnarKeyGroupPartitioner partitioner = new ColumnarKeyGroupPartitioner(128);
    assertEquals(SubtaskStateMapper.RANGE, partitioner.getDownstreamSubtaskStateMapper());
    assertTrue(
        partitioner.isSupportsUnalignedCheckpoint(),
        "each Arrow record contains exactly one topology-independent key group");
  }

  @Test
  void streamGraphAllowsUnalignedColumnarExchange() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    DataStream<ArrowBatch> input =
        env.fromData(1)
            .map(ignored -> (ArrowBatch) null)
            .returns(ArrowBatchTypeInformation.INSTANCE);
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            input.getTransformation(),
            new ColumnarKeyGroupPartitioner(128),
            StreamExchangeMode.PIPELINED);
    new DataStream<>(env, partition).sinkTo(new DiscardingSink<>());

    List<StreamEdge> columnarEdges =
        env.getStreamGraph().getStreamNodes().stream()
            .flatMap(node -> node.getOutEdges().stream())
            .filter(edge -> edge.getPartitioner() instanceof ColumnarKeyGroupPartitioner)
            .toList();
    assertEquals(1, columnarEdges.size());
    assertTrue(columnarEdges.get(0).supportsUnalignedCheckpoints());
  }

  @Test
  void stateKeySelectorRepresentsTheExactTaggedKeyGroup() throws Exception {
    int maxParallelism = 128;
    ArrowBatchSubtaskKeySelector selector = new ArrowBatchSubtaskKeySelector(maxParallelism);
    for (int keyGroup = 0; keyGroup < maxParallelism; keyGroup++) {
      int stateKey = selector.getKey(new ArrowBatch(null, keyGroup));
      assertEquals(
          keyGroup,
          KeyGroupRangeAssignment.computeKeyGroupForKeyHash(stateKey, maxParallelism));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void routesNestedMapKeysByFlinkKeyGroup() throws Exception {
    int channels = 4;
    int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(channels);
    List<RowData> rows = List.of(nestedRow("a", -1), nestedRow("long-key", 7), nestedRow("a", -1));
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new SplitByKeyGroupOperator(
                    new int[] {0}, new int[] {-1, -1, -1, 9}, maxParallelism),
                new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(rows, NESTED_SCHEMA, allocator))));

      RowDataSerializer serializer = new RowDataSerializer(RowType.of(NESTED_SCHEMA.getTypeAt(0)));
      int total = 0;
      for (Object record : harness.getOutput()) {
        if (!(record instanceof StreamRecord)) {
          continue;
        }
        ArrowBatch batch = ((StreamRecord<ArrowBatch>) record).getValue();
        try (VectorSchemaRoot sub = batch.root()) {
          for (RowData row : RowDataArrowConverter.read(sub, NESTED_SCHEMA)) {
            GenericRowData projected = GenericRowData.of(row.getMap(0));
            int group =
                KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                    serializer.toBinaryRow(projected).hashCode(), maxParallelism);
            assertEquals(group, batch.keyGroup());
            total++;
          }
        }
      }
      assertEquals(rows.size(), total);
    }
  }

  private static RowData nestedRow(String key, long epochMillis) {
    LinkedHashMap<StringData, Object> map = new LinkedHashMap<>();
    map.put(
        StringData.fromString(key),
        new GenericArrayData(
            new TimestampData[] {
              TimestampData.fromEpochMillis(epochMillis, epochMillis < 0 ? 999_999 : 42), null
            }));
    return GenericRowData.of(new GenericMapData(map), 1);
  }
}
