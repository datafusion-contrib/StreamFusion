package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import org.apache.arrow.vector.ipc.*;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BinaryEltAdmissionBenchmark {
  private static volatile long consumed;

  @Test
  void releasedCalcConsumesPreparedBinaryRows() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
    int rows = Integer.getInteger("binary.admission.rows", 131072);
    int width = Integer.getInteger("binary.admission.width", 64);
    int warmups = Integer.getInteger("binary.admission.warmups", 5);
    int runs = Integer.getInteger("binary.admission.runs", 9);
    var dataType =
        DataTypes.ROW(
            DataTypes.FIELD("n", DataTypes.INT()),
            DataTypes.FIELD("a", DataTypes.BINARY(4)),
            DataTypes.FIELD("b", DataTypes.BINARY(4)));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            env.fromElements(Row.of(1, new byte[4], new byte[4]))
                .returns(ExternalTypeInfo.<Row>of(dataType)),
            Schema.newBuilder().fromRowDataType(dataType).build()));
    var query = table.sqlQuery("SELECT ELT(n,a,b) FROM src");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    OneInputTransformation<?, ?> calc = null;
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      if (node instanceof OneInputTransformation && node.getName().contains("Calc")) {
        calc = (OneInputTransformation<?, ?>) node;
        break;
      }
      pending.addAll(node.getInputs());
    }
    assertNotNull(calc, "stock generated Calc missing");
    var inputType = ((InternalTypeInfo<?>) calc.getInputs().get(0).getOutputType()).toRowType();
    var outputType = ((InternalTypeInfo<?>) calc.getOutputType()).toRowType();
    byte[] factory = InstantiationUtil.serializeObject(calc.getOperatorFactory());
    var serializer = new RowDataSerializer(inputType);
    List<RowData> input = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++) {
      Integer index = i % 6 == 0 ? null : new int[] {-1, 1, 2, 3, 0}[i % 5];
      input.add(
          serializer
              .toBinaryRow(
                  GenericRowData.of(
                      index,
                      i % 7 == 0 ? null : bytes(i, width),
                      i % 11 == 0 ? null : bytes(i + 1, width + 5)))
              .copy());
    }
    String export = System.getProperty("binary.admission.export");
    if (export != null)
      try (var root = RowDataArrowConverter.write(input, inputType, NativeAllocator.SHARED);
          var channel =
              FileChannel.open(
                  Path.of(export),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING,
                  StandardOpenOption.WRITE);
          var writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    List<String> oracle = null;
    String output = System.getProperty("binary.admission.rustOutput");
    if (output != null) {
      oracle = new ArrayList<>(rows);
      try (var channel = FileChannel.open(Path.of(output));
          var reader = new ArrowFileReader(channel, NativeAllocator.SHARED)) {
        while (reader.loadNextBatch()) {
          var root = reader.getVectorSchemaRoot();
          for (int i = 0; i < root.getRowCount(); i++)
            oracle.add(text((byte[]) root.getVector(0).getObject(i)));
        }
      }
    }
    var records = new ArrayList<StreamRecord<RowData>>(rows);
    for (RowData row : input) records.add(new StreamRecord<>(row));
    Long expected = null;
    for (int trial = 0; trial < warmups + runs; trial++) {
      var digest = new Digest(trial == 0);
      try (var harness = harness(factory, outputType, digest)) {
        for (int i = 0; i < Math.min(1024, rows); i++) harness.processElement(records.get(i));
        digest.reset();
        long start = System.nanoTime();
        for (var record : records) harness.processElement(record);
        consumed ^= digest.hash;
        double seconds = (System.nanoTime() - start) / 1e9;
        assertEquals(rows, digest.count);
        if (expected == null) expected = digest.hash;
        else assertEquals(expected.longValue(), digest.hash);
        if (trial == 0 && oracle != null) assertEquals(oracle, digest.values);
        System.out.printf(
            Locale.ROOT,
            "[binary-elt-admission] trial=%d warmup=%s seconds=%.9f rows=%d width=%d checksum=%d%n",
            trial,
            trial < warmups,
            seconds,
            rows,
            width,
            digest.hash);
      }
    }
  }

  private static byte[] bytes(int id, int width) {
    int length = id % 127 == 0 ? 4097 : id % 3 == 0 ? 0 : width + (id % 3 == 1 ? 0 : 3);
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) result[i] = (byte) (id + i);
    return result;
  }

  private static String text(byte[] bytes) {
    return "+I:"
        + (bytes == null ? "NULL" : org.apache.flink.util.StringUtils.byteToHexString(bytes));
  }

  private static final class Digest {
    long hash;
    int count;
    final List<String> values;

    Digest(boolean capture) {
      values = capture ? new ArrayList<>() : null;
    }

    void reset() {
      hash = 0;
      count = 0;
      if (values != null) values.clear();
    }

    void add(RowData row) {
      byte[] value = row.isNullAt(0) ? null : row.getBinary(0);
      hash += value == null ? -1 : value.length;
      count++;
      if (values != null)
        values.add(
            row.getRowKind().shortString()
                + ":"
                + (value == null
                    ? "NULL"
                    : org.apache.flink.util.StringUtils.byteToHexString(value)));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static OneInputStreamOperatorTestHarness<RowData, RowData> harness(
      byte[] encoded, RowType outputType, Digest digest) throws Exception {
    StreamOperatorFactory factory =
        InstantiationUtil.deserializeObject(
            encoded, BinaryEltAdmissionBenchmark.class.getClassLoader());
    var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 128, 1, 0);
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
}
