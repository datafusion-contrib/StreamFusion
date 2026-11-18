package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

class SplitByKeyGroupOperatorTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void projectedKeysRetainReleasedCompositeNullSlotRouting(boolean recoverable) throws Exception {
    var schema =
        RowType.of(
            new org.apache.flink.table.types.logical.DecimalType(38, 18),
            new TimestampType(9),
            new VarCharType(100),
            new IntType());
    int[] fields = {0, 1, 2};
    var selector =
        org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(),
            fields,
            org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(schema));
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < 96; i++)
      rows.add(
          GenericRowData.of(
              i % 3 == 0
                  ? null
                  : org.apache.flink.table.data.DecimalData.fromBigDecimal(
                      new java.math.BigDecimal(i - 48).movePointLeft(18), 38, 18),
              i % 5 == 0 ? null : TimestampData.fromEpochMillis(i - 48, i * 997),
              i % 7 == 0 ? null : StringData.fromString(i % 2 == 0 ? "Aa" : "BB"),
              i));
    try (var allocator = new RootAllocator();
        var harness =
            new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
                new SplitByKeyGroupOperator(
                    fields, new int[] {-1, 9, -1}, 128, recoverable, new int[0], true),
                new ArrowBatchSerializer())) {
      ColumnarShuffleTestOutput.configure(harness, 3, 128, recoverable);
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(new ArrowBatch(RowDataArrowConverter.write(rows, schema, allocator))));
      var seen = new java.util.HashSet<Integer>();
      for (var item : harness.getOutput()) {
        if (!(item instanceof StreamRecord<?>)) continue;
        var batch = ((StreamRecord<ArrowBatch>) item).getValue();
        try (var root = batch.root()) {
          int last = -1;
          int ordinal = 0;
          for (var row : RowDataArrowConverter.read(root, schema)) {
            int id = row.getInt(3);
            assertTrue(seen.add(id));
            assertTrue(id > last, "retain source order within each destination");
            last = id;
            int keyGroup =
                KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                    selector.getKey(rows.get(id)).hashCode(), 128);
            if (recoverable) {
              assertEquals(keyGroup, batch.keyGroup());
              assertEquals(id, batch.rowOrdinals()[ordinal++]);
            } else assertEquals(keyGroup * 3 / 128, batch.keyGroup() * 3 / 128);
          }
        }
      }
      assertEquals(96, seen.size());
    }
  }
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsLateParallelismChangesForSingleton(boolean recoverable) throws Exception {
    try (var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new SplitByKeyGroupOperator(new int[0], new int[0], 128, recoverable))) {
      var output =
          ColumnarShuffleTestOutput.output(
              32, 128, ColumnarKeyGroupPartitioner.singleton(128, recoverable));
      ColumnarShuffleTestOutput.configure(
          harness, List.of(output), List.of(output), List.of(ColumnarShuffleTestOutput.writer(32, 128)));
      harness.setup(new ArrowBatchSerializer());
      var error = assertThrows(IllegalStateException.class, harness::open);
      assertTrue(error.getMessage().contains("exactly one downstream channel, deployed 32"));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 4})
  @SuppressWarnings("unchecked")
  void splitsABatchIntoKeyGroupTaggedSubBatches(int channels) throws Exception {
    int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(channels);
    int n = 500;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new SplitByKeyGroupOperator(
                    new int[] {0}, new int[] {-1}, maxParallelism),
                new ArrowBatchSerializer())) {
      ColumnarShuffleTestOutput.configure(harness, channels, maxParallelism, false);
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      List<RowData> rows = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        rows.add(row(i % 53, i));
      }
      VectorSchemaRoot in = RowDataArrowConverter.write(rows, SCHEMA, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(in)));

      int total = 0;
      int emitted = 0;
      Map<Long, Integer> keyToGroup = new HashMap<>();
      RowDataSerializer serializer = new RowDataSerializer(RowType.of(new BigIntType()));
      for (Object record : harness.getOutput()) {
        if (!(record instanceof StreamRecord)) {
          continue;
        }
        ArrowBatch batch = ((StreamRecord<ArrowBatch>) record).getValue();
        emitted++;
        int representativeGroup = batch.keyGroup();
        assertTrue(
            representativeGroup >= 0 && representativeGroup < maxParallelism,
            "key group in range");
        int channel =
            KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                maxParallelism, channels, representativeGroup);
        try (VectorSchemaRoot sub = batch.root()) {
          for (RowData r : RowDataArrowConverter.read(sub, SCHEMA)) {
            long key = r.getLong(0);
            GenericRowData projectedKey = new GenericRowData(1);
            projectedKey.setField(0, key);
            int keyGroup =
                KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                    serializer.toBinaryRow(projectedKey).hashCode(), maxParallelism);
            assertEquals(
                channel,
                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                    maxParallelism, channels, keyGroup),
                "destination channel for key " + key);
            Integer prev = keyToGroup.put(key, keyGroup);
            if (prev != null) {
              assertEquals(prev.intValue(), keyGroup, "key " + key + " changed key group");
            }
            total++;
          }
        }
      }
      assertEquals(n, total, "all rows preserved");
      assertTrue(emitted <= channels, "at most one batch per destination channel");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  @SuppressWarnings("unchecked")
  void recoverableSplitEmitsOneOrderedFragmentPerKeyGroup(int channels) throws Exception {
    int maxParallelism = 128;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new SplitByKeyGroupOperator(
                    new int[] {0}, new int[] {-1}, maxParallelism, true),
                new ArrowBatchSerializer())) {
      ColumnarShuffleTestOutput.configure(harness, channels, maxParallelism, true);
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      List<RowData> rows = List.of(row(1, 0), row(2, 1), row(1, 2), row(3, 3), row(2, 4));
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(rows, SCHEMA, allocator))));

      Map<Integer, ArrowBatch> fragments = new HashMap<>();
      int total = 0;
      for (Object item : harness.getOutput()) {
        if (item instanceof StreamRecord<?>) {
          ArrowBatch fragment = ((StreamRecord<ArrowBatch>) item).getValue();
          assertTrue(fragment.isOrderedKeyGroupFragment());
          assertEquals(null, fragments.put(fragment.keyGroup(), fragment));
          assertEquals(fragment.rowCount(), fragment.rowOrdinals().length);
          total += fragment.rowCount();
        }
      }
      assertEquals(rows.size(), total);
      List<Integer> parentKeyGroups =
          fragments.keySet().stream().sorted().collect(ListCollectors.toList());
      for (ArrowBatch fragment : fragments.values()) {
        assertEquals(
            parentKeyGroups,
            java.util.Arrays.stream(fragment.parentKeyGroups())
                .boxed()
                .sorted()
                .collect(ListCollectors.toList()));
      }
      for (ArrowBatch fragment : fragments.values()) {
        fragment.root().close();
      }
    }
  }

  @Test
  void singleChannelConsumesAnEmptyBatchWithoutEmitting() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, 128),
                new ArrowBatchSerializer())) {
      ColumnarShuffleTestOutput.configure(harness, 1, 128, false);
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      VectorSchemaRoot empty = RowDataArrowConverter.write(List.of(), SCHEMA, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(empty)));
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(0, allocator.getAllocatedMemory());
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

    assertEquals(
        p.selectChannel(delegate),
        partitioner.copy().selectChannel(delegate),
        "Flink 1.18's recovery filter uses the configured copy without another setup call");

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
  void streamGraphAllowsRecoverableColumnarExchangeChannelState() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    DataStream<ArrowBatch> input =
        fromData(env, 1)
            .map(ignored -> (ArrowBatch) null)
            .returns(ArrowBatchTypeInformation.INSTANCE);
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            input.getTransformation(),
            new ColumnarKeyGroupPartitioner(128, true),
            StreamExchangeMode.PIPELINED);
    tech.streamfusion.compat.StreamTestSinks.discard(new DataStream<>(env, partition));
    StreamEdge edge =
        env.getStreamGraph().getStreamNodes().stream()
            .flatMap(node -> node.getOutEdges().stream())
            .filter(candidate -> candidate.getPartitioner() instanceof ColumnarKeyGroupPartitioner)
            .findFirst()
            .orElseThrow();
    assertTrue(edge.supportsUnalignedCheckpoints());
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
      ColumnarShuffleTestOutput.configure(harness, channels, maxParallelism, false);
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
        int channel =
            KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                maxParallelism, channels, batch.keyGroup());
        try (VectorSchemaRoot sub = batch.root()) {
          for (RowData row : RowDataArrowConverter.read(sub, NESTED_SCHEMA)) {
            GenericRowData projected = GenericRowData.of(row.getMap(0));
            int group =
                KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                    serializer.toBinaryRow(projected).hashCode(), maxParallelism);
            assertEquals(
                channel,
                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                    maxParallelism, channels, group));
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
