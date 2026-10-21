package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.NativePlanner;

/** Java/RowData control; the matching Arrow execution is timed by the standalone Rust driver. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class FloatingDistinctPartialsAdmissionBenchmark {
  @ParameterizedTest
  @CsvSource({
    "float,sum",
    "float,avg",
    "float,shared",
    "double,sum",
    "double,avg",
    "double,shared"
  })
  void releasedLocalAndGlobalOperatorsReceivePreparedRows(String type, String shape)
      throws Exception {
    int rows = Integer.getInteger("partials.rows", 131072);
    int keys = Integer.getInteger("partials.keys", 64);
    int cardinality = Integer.getInteger("partials.cardinality", 1024);
    int bundle = Integer.getInteger("partials.bundle", 1024);
    int warmups = Integer.getInteger("partials.warmups", 3);
    int runs = Integer.getInteger("partials.runs", 7);
    var stock = stages(type, shape, bundle, false);
    var nativeStages = stages(type, shape, bundle, true);
    RowType inputType =
        ((InternalTypeInfo<?>) stock[0].getInputs().get(0).getOutputType()).toRowType();
    RowType partialType = ((InternalTypeInfo<?>) stock[0].getOutputType()).toRowType();
    RowType outputType = ((InternalTypeInfo<?>) stock[1].getOutputType()).toRowType();
    assertEquals("k", inputType.getFieldNames().get(0));
    var serializer = new RowDataSerializer(inputType);
    var input = new ArrayList<RowData>(rows);
    for (int i = 0; i < rows; i++) {
      var row = new GenericRowData(inputType.getFieldCount());
      row.setField(0, i % keys);
      double value = (i / keys % cardinality - cardinality / 2) * 0.25;
      for (int column = 1; column < row.getArity(); column++) {
        if (i % 7 == 0) continue;
        switch (inputType.getTypeAt(column).getTypeRoot()) {
          case FLOAT:
            row.setField(column, (float) value);
            break;
          case DOUBLE:
            row.setField(column, value);
            break;
          default:
            throw new AssertionError(inputType);
        }
      }
      input.add(serializer.toBinaryRow(row).copy());
    }
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("rows", rows);
    manifest.put("shape", shape);
    manifest.put("keys", keys);
    manifest.put("cardinality", cardinality);
    manifest.put("bundle", bundle);
    manifest.put(
        "output_types",
        outputType.getChildren().stream()
            .map(t -> t.getTypeRoot().name())
            .collect(java.util.stream.Collectors.toList()));
    for (int stage = 0; stage < 2; stage++) {
      Object operator =
          ((SimpleOperatorFactory<?>) nativeStages[stage].getOperatorFactory()).getOperator();
      var config = new LinkedHashMap<String, Object>();
      for (String field :
          new String[] {
            "aggregateKinds",
            "valueTypes",
            "valueColumns",
            "keyColumns",
            "filterColumns",
            stage == 0 ? "distinctViewSources" : "distinctViewColumns"
          }) {
        config.put(field, field(operator, field));
      }
      if (stage == 1) {
        config.put("countColumns", field(operator, "countColumns"));
        assertEquals(true, field(operator, "generateUpdateBefore"));
      }
      manifest.put(stage == 0 ? "local" : "global", config);
    }
    String export = System.getProperty("partials.export");
    if (export != null) {
      try (var allocator = new RootAllocator();
          var root = RowDataArrowConverter.write(input, inputType, allocator);
          var channel =
              FileChannel.open(
                  Path.of(export + "-" + type + "-" + shape + ".arrow"),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING,
                  StandardOpenOption.WRITE);
          var writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    }
    long expectedRows = -1;
    long expectedHash = 0;
    var records = new ArrayList<StreamRecord<RowData>>(input.size());
    for (RowData row : input) records.add(new StreamRecord<>(row));
    for (int trial = 0; trial < warmups + runs; trial++) {
      long[] digest = new long[2];
      try (var global = harness(stock[1], outputType);
          var local = harness(stock[0], partialType)) {
        output(
            global,
            new FlinkOutput<RowData>(null) {
              @Override
              public void collect(StreamRecord<RowData> record) {
                RowData row = record.getValue();
                long hash = row.getRowKind().toByteValue();
                for (int column = 0; column < row.getArity(); column++) {
                  long value = 0x9e3779b97f4a7c15L;
                  if (!row.isNullAt(column)) {
                    switch (outputType.getTypeAt(column).getTypeRoot()) {
                      case INTEGER:
                        value = row.getInt(column);
                        break;
                      case BIGINT:
                        value = row.getLong(column);
                        break;
                      case FLOAT:
                        value =
                            Integer.toUnsignedLong(Float.floatToRawIntBits(row.getFloat(column)));
                        break;
                      case DOUBLE:
                        value = Double.doubleToRawLongBits(row.getDouble(column));
                        break;
                      default:
                        throw new AssertionError(outputType);
                    }
                  }
                  hash = hash * 31 + value;
                }
                digest[0]++;
                digest[1] += hash;
              }
            });
        var transport = new RowDataSerializer(partialType);
        output(
            local,
            new FlinkOutput<RowData>(null) {
              @Override
              public void collect(StreamRecord<RowData> record) {
                try {
                  // The split boundary copies MapView contents before local state is reused.
                  global.processElement(new StreamRecord<>(transport.copy(record.getValue())));
                } catch (Exception failure) {
                  throw new IllegalStateException(failure);
                }
              }
            });
        global.open();
        local.open();
        long start = System.nanoTime();
        for (StreamRecord<RowData> record : records) local.processElement(record);
        local.getOperator().finish();
        global.getOperator().finish();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (expectedRows < 0) {
          expectedRows = digest[0];
          expectedHash = digest[1];
        }
        assertEquals(expectedRows, digest[0]);
        assertEquals(expectedHash, digest[1]);
        System.out.printf(
            Locale.ROOT,
            "[partials-admission] type=%s shape=%s rows=%d keys=%d cardinality=%d bundle=%d"
                + " trial=%d warmup=%s seconds=%.9f output_rows=%d checksum=%d%n",
            type,
            shape,
            rows,
            keys,
            cardinality,
            bundle,
            trial,
            trial < warmups,
            seconds,
            digest[0],
            digest[1]);
      }
    }
    manifest.put("expected_rows", expectedRows);
    manifest.put("expected_checksum", expectedHash);
    if (export != null)
      new ObjectMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValue(Path.of(export + "-" + type + "-" + shape + ".json").toFile(), manifest);
  }

  private static OneInputTransformation<?, ?>[] stages(
      String type, String shape, int bundle, boolean nativeEngine) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
    boolean single = type.equals("float");
    var source =
        env.fromSequence(0, 3)
            .map(
                id -> {
                  if (single) return Row.of(id.intValue(), id.floatValue());
                  return Row.of(id.intValue(), id.doubleValue());
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v"}, Types.INT, single ? Types.FLOAT : Types.DOUBLE));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", single ? DataTypes.FLOAT() : DataTypes.DOUBLE())
            .build());
    if (nativeEngine) NativePlanner.install(table);
    String reductions =
        shape.equals("sum")
            ? "SUM(DISTINCT v)"
            : shape.equals("avg")
                ? "AVG(DISTINCT v)"
                : "SUM(DISTINCT v), AVG(DISTINCT v), COUNT(DISTINCT v)";
    var query = table.sqlQuery("SELECT k, " + reductions + " FROM src GROUP BY k");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    var result = new OneInputTransformation<?, ?>[2];
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      if (node instanceof OneInputTransformation<?, ?>) {
        String local = nativeEngine ? "NativeColumnarLocalGroupAgg" : "LocalGroupAggregate";
        String global = nativeEngine ? "NativeColumnarGroupAgg" : "GlobalGroupAggregate";
        if (node.getName().contains(local)) result[0] = (OneInputTransformation<?, ?>) node;
        else if (node.getName().contains(global)) result[1] = (OneInputTransformation<?, ?>) node;
      }
      pending.addAll(node.getInputs());
    }
    assertNotNull(result[0]);
    assertNotNull(result[1]);
    return result;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static OneInputStreamOperatorTestHarness<RowData, RowData> harness(
      OneInputTransformation stage, RowType output) throws Exception {
    StreamOperatorFactory factory =
        InstantiationUtil.deserializeObject(
            InstantiationUtil.serializeObject(stage.getOperatorFactory()),
            FloatingDistinctPartialsAdmissionBenchmark.class.getClassLoader());
    OneInputStreamOperatorTestHarness harness =
        stage.getStateKeySelector() == null
            ? new OneInputStreamOperatorTestHarness(factory, 128, 1, 0)
            : new KeyedOneInputStreamOperatorTestHarness(
                factory, stage.getStateKeySelector(), stage.getStateKeyType());
    harness.setup(new RowDataSerializer(output));
    return harness;
  }

  private static Object field(Object object, String name) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }

  private static void output(
      OneInputStreamOperatorTestHarness<?, ?> harness, FlinkOutput<RowData> output)
      throws Exception {
    Field field = AbstractStreamOperator.class.getDeclaredField("output");
    field.setAccessible(true);
    field.set(harness.getOperator(), output);
  }
}
