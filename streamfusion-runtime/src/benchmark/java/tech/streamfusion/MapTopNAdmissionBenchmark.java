package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.lang.reflect.Field;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.operator.*;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class MapTopNAdmissionBenchmark {
  private static volatile long consumed;

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void releasedRankOnPreparedRowsIncludesStateAndOutputConsumption() throws Exception {
    int count = Integer.getInteger("map.admission.rows", 65536);
    int keys = Integer.getInteger("map.admission.keys", 4096);
    int width = Integer.getInteger("map.admission.width", 64);
    int warmups = Integer.getInteger("map.admission.warmups", 5);
    int runs = Integer.getInteger("map.admission.runs", 9);
    boolean bag = Boolean.getBoolean("map.admission.multiset");
    String churn = System.getProperty("map.admission.churn", "high");
    var logical =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.BIGINT()),
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD(
                "m",
                bag
                    ? DataTypes.MULTISET(DataTypes.STRING())
                    : DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            fromData(
                env, List.of(Row.of(0L, 0L, Map.of("k", 1))), ExternalTypeInfo.<Row>of(logical)),
            Schema.newBuilder().fromRowDataType(logical).build()));
    var query =
        table.sqlQuery(
            "SELECT k,id,m,rn FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY id DESC) AS"
                + " rn FROM src) WHERE rn<=3");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    OneInputTransformation<?, ?> rank = null;
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      if (node instanceof OneInputTransformation && node.getName().contains("Rank")) {
        rank = (OneInputTransformation<?, ?>) node;
        break;
      }
      pending.addAll(node.getInputs());
    }
    assertNotNull(rank, "released Rank operator missing");
    var inputType = ((InternalTypeInfo<?>) rank.getInputs().get(0).getOutputType()).toRowType();
    var outputType = ((InternalTypeInfo<?>) rank.getOutputType()).toRowType();
    assertEquals(3, inputType.getFieldCount());
    assertEquals(4, outputType.getFieldCount());
    var serializer = new RowDataSerializer(inputType);
    List<RowData> input = new ArrayList<>(count);
    RowData[] byId = new RowData[count];
    for (int i = 0; i < count; i++) {
      int id = churn.equals("high") ? i : count - 1 - i;
      var values = new LinkedHashMap<StringData, Integer>();
      if (id % 17 != 0)
        for (int j = 0; j < width; j++)
          values.put(
              j == 0 && id % 7 == 0 ? null : StringData.fromString("k" + j),
              bag
                  ? Integer.valueOf(1 + (id + j) % 5)
                  : j % 11 == 0 ? null : Integer.valueOf(id + j));
      RowData row =
          serializer
              .toBinaryRow(
                  GenericRowData.of(
                      (long) (i % keys),
                      (long) id,
                      id % 11 == 0 ? null : new GenericMapData(values)))
              .copy();
      input.add(row);
      byId[id] = row;
    }
    String export = System.getProperty("map.admission.export");
    if (export != null) {
      try (var owner = RowDataArrowConverter.write(input, inputType, NativeAllocator.SHARED);
          var channel =
              FileChannel.open(
                  Path.of(export),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING,
                  StandardOpenOption.WRITE)) {
        var view = MapListRepresentation.forExport(owner);
        try (var writer = new ArrowFileWriter(view, null, channel)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
      }
    }
    byte[] factory = InstantiationUtil.serializeObject(rank.getOperatorFactory());
    var golden = new Digest(byId, true);
    try (var harness = harness(rank, factory, outputType, golden)) {
      for (RowData row : input) harness.processElement(new StreamRecord<>(row));
    }
    if (export != null) {
      ByteBuffer buffer =
          ByteBuffer.allocate(golden.counts.length * 4).order(ByteOrder.LITTLE_ENDIAN);
      for (int value : golden.counts) buffer.putInt(value);
      Files.write(Path.of(export + ".counts.bin"), buffer.array());
    }
    for (int trial = 0; trial < warmups + runs; trial++) {
      var digest = new Digest(byId, false);
      try (var harness = harness(rank, factory, outputType, digest)) {
        long clock = System.nanoTime();
        for (RowData row : input) harness.processElement(new StreamRecord<>(row));
        consumed ^= digest.checksum;
        double seconds = (System.nanoTime() - clock) / 1e9;
        assertEquals(golden.rows, digest.rows);
        assertEquals(golden.checksum, digest.checksum);
        System.out.printf(
            Locale.ROOT,
            "[map-topn-admission] trial=%d warmup=%s seconds=%.9f rows=%d checksum=%d%n",
            trial,
            trial < warmups,
            seconds,
            digest.rows,
            digest.checksum);
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static KeyedOneInputStreamOperatorTestHarness harness(
      OneInputTransformation<?, ?> rank, byte[] bytes, RowType outputType, Digest digest)
      throws Exception {
    StreamOperatorFactory factory =
        InstantiationUtil.deserializeObject(
            bytes, MapTopNAdmissionBenchmark.class.getClassLoader());
    var harness =
        new KeyedOneInputStreamOperatorTestHarness(
            factory, rank.getStateKeySelector(), rank.getStateKeyType(), 128, 1, 0);
    harness.setup(new RowDataSerializer(outputType));
    Field output = AbstractStreamOperator.class.getDeclaredField("output");
    output.setAccessible(true);
    output.set(
        harness.getOperator(),
        new FlinkOutput<RowData>(null) {
          @Override
          public void collect(StreamRecord<RowData> record) {
            digest.add(record.getValue());
          }
        });
    harness.open();
    return harness;
  }

  private static final class Digest {
    final RowData[] input;
    final int[] counts;
    long rows, checksum;

    Digest(RowData[] input, boolean verify) {
      this.input = input;
      counts = verify ? new int[input.length * 12] : null;
    }

    void add(RowData row) {
      long id = row.getLong(1), rank = row.getLong(3);
      int kind = row.getRowKind().toByteValue();
      if (counts != null) {
        assertTrue(rank >= 1 && rank <= 3);
        RowData expected = input[(int) id];
        assertEquals(expected.getLong(0), row.getLong(0));
        assertEquals(expected.isNullAt(2), row.isNullAt(2));
        if (!row.isNullAt(2)) assertMaps(expected.getMap(2), row.getMap(2));
        counts[(int) id * 12 + kind * 3 + (int) rank - 1]++;
      }
      long value = row.getLong(0) * 31 + id;
      value = value * 31 + (row.isNullAt(2) ? 0 : hash(row.getMap(2)));
      value = (value * 31 + rank) * 31 + kind;
      checksum += value;
      rows++;
    }
  }

  private static long hash(MapData map) {
    long hash = 1;
    ArrayData keys = map.keyArray(), values = map.valueArray();
    for (int i = 0; i < map.size(); i++) {
      long key = 0;
      if (!keys.isNullAt(i)) {
        key = 1;
        for (byte b : keys.getString(i).toBytes()) key = key * 31 + (b & 255);
      }
      long value = values.isNullAt(i) ? 0 : (long) values.getInt(i) + 1;
      hash = hash * 31 + (31 + key) * 31 + value;
    }
    return hash;
  }

  private static void assertMaps(MapData expected, MapData actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.keyArray().isNullAt(i), actual.keyArray().isNullAt(i));
      if (!expected.keyArray().isNullAt(i))
        assertArrayEquals(
            expected.keyArray().getString(i).toBytes(), actual.keyArray().getString(i).toBytes());
      assertEquals(expected.valueArray().isNullAt(i), actual.valueArray().isNullAt(i));
      if (!expected.valueArray().isNullAt(i))
        assertEquals(expected.valueArray().getInt(i), actual.valueArray().getInt(i));
    }
  }
}
