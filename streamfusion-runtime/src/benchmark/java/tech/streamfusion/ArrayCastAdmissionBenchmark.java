package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
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
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.NativePlanner;

/** Java/RowData half of the functionality gate; Rust consumes the same exported Arrow rows. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ArrayCastAdmissionBenchmark {
  private static volatile long consumed;

  @Test
  void preparedRowsUseTheReleasedGeneratedCast() throws Exception {
    String shape = System.getProperty("array.cast.shape", "int-bigint");
    boolean decimal = shape.equals("decimal-rescale");
    var element = decimal ? DECIMAL(18, 2) : INT();
    String target =
        shape.equals("int-bigint")
            ? "ARRAY<BIGINT>"
            : decimal ? "ARRAY<DECIMAL(18,1)>" : "ARRAY<DECIMAL(18,2)>";
    int rows = Integer.getInteger("array.cast.rows", 131072);
    int width = Integer.getInteger("array.cast.width", 32);
    int warmups = Integer.getInteger("array.cast.warmups", 3);
    int runs = Integer.getInteger("array.cast.runs", 7);
    String mode = System.getProperty("array.cast.mode", "stock");
    var type = ROW(FIELD("a", ARRAY(element)));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            env.fromElements(Row.of((Object) null)).returns(ExternalTypeInfo.<Row>of(type)),
            Schema.newBuilder().fromRowDataType(type).build()));
    var scan = mode.equals("previous") ? NativePlanner.install(table) : null;
    var query = table.sqlQuery("SELECT CAST(a AS " + target + ") FROM src");
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
    assertNotNull(calc);
    if (scan != null) {
      assertEquals(0, scan.substitutions(), scan.explainSummary());
      assertTrue(
          scan.fallbackReasons().stream().anyMatch(s -> s.contains("CAST ARRAY")),
          scan.explainSummary());
    }
    var inputType = ((InternalTypeInfo<?>) calc.getInputs().get(0).getOutputType()).toRowType();
    var outputType = ((InternalTypeInfo<?>) calc.getOutputType()).toRowType();
    byte[] factory = InstantiationUtil.serializeObject(calc.getOperatorFactory());
    var serializer = new RowDataSerializer(inputType);
    var input = new ArrayList<RowData>(rows);
    for (int i = 0; i < rows; i++) {
      Object[] values = decimal ? new DecimalData[width] : new Integer[width];
      for (int j = 0; j < width; j++) {
        long value = ((long) i * 7 + j * 3) % 200001 - 100000;
        values[j] =
            j % 11 == 0
                ? null
                : decimal
                    ? DecimalData.fromBigDecimal(BigDecimal.valueOf(value, 2), 18, 2)
                    : (int) value;
      }
      ArrayData array =
          i % 19 == 0
              ? null
              : new GenericArrayData(
                  i % 23 == 0 ? (decimal ? new DecimalData[] {} : new Integer[] {}) : values);
      input.add(serializer.toBinaryRow(GenericRowData.of(array)).copy());
    }
    String export = System.getProperty("array.cast.export");
    if (export != null) {
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
      Files.writeString(
          Path.of(export + ".target"),
          Base64.getEncoder()
              .encodeToString(ArrowConversion.toArrowSchema(outputType).serializeAsMessage()));
    }
    var records = new ArrayList<StreamRecord<RowData>>(rows);
    for (var row : input) records.add(new StreamRecord<>(row));
    Long expected = null;
    for (int trial = 0; trial < warmups + runs; trial++) {
      var digest = new Digest(((ArrayType) outputType.getTypeAt(0)).getElementType());
      try (var harness = harness(factory, outputType, digest)) {
        for (int i = 0; i < Math.min(1024, rows); i++) harness.processElement(records.get(i));
        digest.hash = 0;
        digest.count = 0;
        long start = System.nanoTime();
        for (var record : records) harness.processElement(record);
        double seconds = (System.nanoTime() - start) / 1e9;
        consumed ^= digest.hash;
        assertEquals(rows, digest.count);
        if (expected == null) expected = digest.hash;
        else assertEquals(expected.longValue(), digest.hash);
        System.out.printf(
            Locale.ROOT,
            "[array-cast-admission] mode=%s shape=%s trial=%d"
                + " warmup=%s seconds=%.9f rows=%d width=%d checksum=%d%n",
            mode,
            shape,
            trial,
            trial < warmups,
            seconds,
            rows,
            width,
            digest.hash);
      }
    }
  }

  private static final class Digest {
    final LogicalType element;
    long hash;
    int count;

    Digest(LogicalType element) {
      this.element = element;
    }

    void add(RowData row) {
      long h = -1;
      if (!row.isNullAt(0)) {
        ArrayData a = row.getArray(0);
        h = a.size() + 1;
        for (int i = 0; i < a.size(); i++) {
          long v = 0;
          if (!a.isNullAt(i)) {
            if (element instanceof DecimalType) {
              var d = (DecimalType) element;
              v = a.getDecimal(i, d.getPrecision(), d.getScale()).toUnscaledLong();
            } else v = a.getLong(i);
          }
          h = h * 31 + v;
        }
      }
      hash += h;
      count++;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static OneInputStreamOperatorTestHarness<RowData, RowData> harness(
      byte[] factory, RowType output, Digest digest) throws Exception {
    StreamOperatorFactory op =
        InstantiationUtil.deserializeObject(
            factory, ArrayCastAdmissionBenchmark.class.getClassLoader());
    var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(op, 128, 1, 0);
    harness.setup(new RowDataSerializer(output));
    Field field = AbstractStreamOperator.class.getDeclaredField("output");
    field.setAccessible(true);
    field.set(
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
