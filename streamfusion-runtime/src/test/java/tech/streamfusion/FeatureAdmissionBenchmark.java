package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.operators.misc.DropUpdateBeforeFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.operator.*;
import tech.streamfusion.planner.NativePlanner;

/**
 * Flink's Java/RowData half of small-functionality admission; exports matching Arrow input for the
 * Rust driver. Optional native/both modes measure Java wrappers and are separate diagnostics.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class FeatureAdmissionBenchmark {
  private static volatile long consumed;

  static Stream<String> cases() {
    return Arrays.stream(
        System.getProperty(
                "admission.cases",
                "drop,shared-int,shared-long,shared-text,temporal-identity,temporal-text,temporal-epoch,string-tumble,string-hop,string-cumulate,attached-sum,attached-count-wide,attached-regional")
            .split(","));
  }

  private static boolean isWindow(String name) {
    return name.startsWith("string-") || name.startsWith("count-");
  }

  @ParameterizedTest
  @MethodSource("cases")
  void eachEngineReceivesItsOwnFormatWithEqualLogicalRows(String name) throws Exception {
    System.out.printf(
        "[feature-admission-config] case=%s backend=%s stock_layout=%s direct_temporal=%s"
            + " temporal_aliases=%s%n",
        name,
        System.getProperty("admission.backend", "memory"),
        System.getProperty("admission.stockLayout", "binary"),
        Boolean.parseBoolean(
            System.getProperty("streamfusion.experimental.directTemporalCalc", "true")),
        Boolean.parseBoolean(
            System.getProperty("streamfusion.experimental.temporalAliases", "true")));
    int count = Integer.getInteger("admission.rows", 131072);
    int batchRows = Integer.getInteger("admission.batchRows", 1024);
    int keys = Integer.getInteger("admission.keys", 64);
    int width = Integer.getInteger("admission.width", 64);
    int warmups = Integer.getInteger("admission.warmup", 3);
    int runs = Integer.getInteger("admission.runs", 7);
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    Configuration memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(memory);
    Plan stock = plan(name, false, null);
    if (name.startsWith("over-map-"))
      assertEquals(
          "stock",
          System.getProperty("admission.engines", "stock"),
          "OVER admission uses the separate Rust driver");
    Plan[] plans = {stock, name.startsWith("over-map-") ? stock : plan(name, true, stock.input)};
    assertEquals(plans[0].output.getChildren(), plans[1].output.getChildren());
    System.out.printf("[feature-admission-types] input=%s output=%s%n", stock.input, stock.output);
    var input = rows(name, count, keys, width, plans[0].input);
    if (plans[0].transformation != null && plans[0].transformation.getStateKeySelector() != null) {
      @SuppressWarnings("unchecked")
      KeySelector<RowData, Object> key =
          (KeySelector<RowData, Object>) plans[0].transformation.getStateKeySelector();
      var partitions = new TreeMap<Integer, List<RowData>>();
      for (RowData row : input) {
        int group =
            org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignToKeyGroup(
                key.getKey(row), 128);
        partitions.computeIfAbsent(group, ignored -> new ArrayList<>()).add(row);
      }
      input =
          partitions.values().stream()
              .flatMap(List::stream)
              .collect(java.util.stream.Collectors.toList());
    }
    String stockLayout = System.getProperty("admission.stockLayout", "binary");
    if (stockLayout.equals("binary")) {
      var serializer = new RowDataSerializer(plans[0].input);
      var binary = new ArrayList<RowData>(input.size());
      for (RowData row : input) binary.add(serializer.toBinaryRow(row).copy());
      input = binary;
    } else if (!stockLayout.equals("generic")) throw new IllegalArgumentException(stockLayout);
    Map<String, Long> expected = null;
    Long verifiedHash = null;
    Long verifiedCount = null;
    double[][] seconds = new double[2][runs];
    try (var template =
        RowDataArrowConverter.write(
            input, plans[1].input, NativeAllocator.SHARED, name.equals("drop"))) {
      String export = System.getProperty("admission.exportArrow");
      if (export != null) {
        assertTrue(
            isWindow(name) || name.equals("drop") || name.startsWith("over-map-"),
            "Rust fixture requires a supported window, OVER or changelog case");
        try (var channel =
                java.nio.channels.FileChannel.open(
                    java.nio.file.Path.of(export),
                    java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
                    java.nio.file.StandardOpenOption.WRITE);
            var writer = new org.apache.arrow.vector.ipc.ArrowFileWriter(template, null, channel)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
        var lengths = new ArrayList<Integer>();
        for (int start = 0; start < count; ) {
          int end = Math.min(start + batchRows, count);
          int group = keyGroup(stock, input.get(start));
          for (int index = start + 1; index < end; index++) {
            if (keyGroup(stock, input.get(index)) != group) {
              end = index;
              break;
            }
          }
          lengths.add(end - start);
          start = end;
        }
        java.nio.file.Files.writeString(
            java.nio.file.Path.of(export + ".batches.json"), lengths.toString());
      }
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) & 1;
          String engines = System.getProperty("admission.engines", "stock");
          if (engines.equals("stock") && engine != 0 || engines.equals("native") && engine != 1)
            continue;
          Plan plan = plans[engine];
          Digest digest = new Digest(plan.output, trial == 0);
          try (OneInputStreamOperatorTestHarness<Object, Object> harness = harness(plan, digest)) {
            List<StreamRecord<Object>> prepared = new ArrayList<>();
            if (engine == 0) {
              for (RowData row : input) prepared.add(new StreamRecord<>(row));
            } else {
              for (int start = 0; start < count; ) {
                int end = Math.min(start + batchRows, count);
                int group = -1;
                if (plans[0].transformation != null
                    && plans[0].transformation.getStateKeySelector() != null) {
                  @SuppressWarnings("unchecked")
                  KeySelector<RowData, Object> key =
                      (KeySelector<RowData, Object>) plans[0].transformation.getStateKeySelector();
                  group =
                      org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignToKeyGroup(
                          key.getKey(input.get(start)), 128);
                  for (int i = start + 1; i < end; i++) {
                    int next =
                        org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignToKeyGroup(
                            key.getKey(input.get(i)), 128);
                    if (next != group) {
                      end = i;
                      break;
                    }
                  }
                }
                prepared.add(
                    new StreamRecord<>(new ArrowBatch(template.slice(start, end - start), group)));
                start = end;
              }
            }
            int warmRows = Math.min(batchRows, count);
            int warmGroup = keyGroup(plans[0], input.get(0));
            if (warmGroup >= 0) {
              for (int index = 1; index < warmRows; index++) {
                if (keyGroup(plans[0], input.get(index)) != warmGroup) {
                  warmRows = index;
                  break;
                }
              }
            }
            if (engine == 0) {
              for (int index = 0; index < warmRows; index++)
                harness.processElement(prepared.get(index));
            } else
              harness.processElement(
                  new StreamRecord<>(new ArrowBatch(template.slice(0, warmRows), warmGroup)));
            digest.count = 0;
            digest.hash = 0;
            if (digest.values != null) digest.values.clear();
            long start = System.nanoTime();
            for (StreamRecord<Object> record : prepared) harness.processElement(record);
            if ((isWindow(name) || name.startsWith("attached-")))
              harness.processWatermark(Watermark.MAX_WATERMARK);
            double elapsed = (System.nanoTime() - start) / 1e9;
            consumed ^= digest.hash + digest.count;
            if (verifiedHash == null) {
              verifiedHash = digest.hash;
              verifiedCount = digest.count;
            } else {
              assertEquals(
                  verifiedHash.longValue(), digest.hash, "checksum versus full-value warmup");
              assertEquals(
                  verifiedCount.longValue(), digest.count, "row count versus full-value warmup");
            }
            if (trial == 0) {
              if (engine == 0) expected = digest.values;
              else if (expected != null) assertEquals(expected, digest.values, name);
              String oracle =
                  System.getProperty(
                      "admission.rustOutput." + name, System.getProperty("admission.rustOutput"));
              if (engine == 0 && oracle != null) {
                Digest nativeDigest = new Digest(plan.output, true);
                try (var channel =
                        java.nio.channels.FileChannel.open(java.nio.file.Path.of(oracle));
                    var reader =
                        new org.apache.arrow.vector.ipc.ArrowFileReader(
                            channel, NativeAllocator.SHARED)) {
                  while (reader.loadNextBatch()) {
                    var root = reader.getVectorSchemaRoot();
                    var rows = RowDataArrowConverter.read(root, plan.output);
                    for (int i = 0; i < rows.size(); i++) nativeDigest.add(rows.get(i), root, i);
                  }
                }
                assertEquals(
                    digest.values, nativeDigest.values, "Rust/Arrow output versus Java/RowData");
              }
            }
            if (trial >= warmups) seconds[engine][trial - warmups] = elapsed;
            System.out.printf(
                Locale.ROOT,
                "[feature-admission-trial] case=%s engine=%s rows=%d batch_rows=%d keys=%d width=%d"
                    + " trial=%d warmup=%s seconds=%.9f output_rows=%d checksum=%d%n",
                name,
                engine == 0 ? "Flink" : "StreamFusion",
                count,
                batchRows,
                keys,
                width,
                trial,
                trial < warmups,
                elapsed,
                digest.count,
                digest.hash);
          }
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[feature-admission] case=%s rows=%d batch_rows=%d keys=%d width=%d stock=%.9f native=%.9f"
            + " stock_trials=%s native_trials=%s%n",
        name,
        count,
        batchRows,
        keys,
        width,
        System.getProperty("admission.engines", "stock").equals("native")
            ? Double.NaN
            : median(seconds[0]),
        System.getProperty("admission.engines", "stock").equals("stock")
            ? Double.NaN
            : median(seconds[1]),
        System.getProperty("admission.engines", "stock").equals("native")
            ? "[]"
            : Arrays.toString(seconds[0]),
        System.getProperty("admission.engines", "stock").equals("stock")
            ? "[]"
            : Arrays.toString(seconds[1]));
  }

  private static final class Plan {
    final boolean nativeEngine;
    final RowType input;
    final RowType output;
    final byte[] factory;
    final OneInputTransformation<?, ?> transformation;

    Plan(
        boolean nativeEngine,
        RowType input,
        RowType output,
        StreamOperatorFactory<?> factory,
        OneInputTransformation<?, ?> transformation)
        throws Exception {
      this.nativeEngine = nativeEngine;
      this.input = input;
      this.output = output;
      this.factory = InstantiationUtil.serializeObject(factory);
      this.transformation = transformation;
    }
  }

  private static Plan plan(String name, boolean nativeEngine, RowType suppliedInput)
      throws Exception {
    DataType input = inputType(name);
    RowType rowType = (RowType) input.getLogicalType();
    if (name.equals("drop")) {
      StreamOperatorFactory<?> factory =
          nativeEngine
              ? SimpleOperatorFactory.of(new NativeColumnarDropUpdateBeforeOperator())
              : SimpleOperatorFactory.of(new StreamFilter<RowData>(new DropUpdateBeforeFunction()));
      return new Plan(nativeEngine, rowType, rowType, factory, null);
    }
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setMaxParallelism(128);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("streamfusion.calc.sharedStatefulUdf.enabled", "true");
    table.getConfig().set("streamfusion.calc.temporalUdf.enabled", "true");
    table.getConfig().set("streamfusion.window.string-extrema.single-phase.enabled", "true");
    table.getConfig().set("streamfusion.window.attached.single-phase.enabled", "true");
    if (name.equals("attached-regional"))
      table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/Los_Angeles"));
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    Row dummy =
        name.startsWith("over-map-")
            ? Row.of(0L, Map.of("k", 1L))
            : name.startsWith("count-")
                ? Row.of(
                    0L,
                    new byte[0],
                    java.time.Duration.ZERO,
                    java.time.Period.ZERO,
                    java.time.Instant.EPOCH)
                : name.startsWith("attached-")
                    ? Row.of(0, 1L, java.time.Instant.EPOCH)
                    : isWindow(name)
                        ? Row.of(0L, "x", java.time.Instant.EPOCH)
                        : name.startsWith("temporal-")
                            ? Row.of(
                                0,
                                java.time.LocalDateTime.of(2020, 1, 1, 0, 0),
                                java.time.Instant.EPOCH)
                            : Row.of(0);
    var source = env.fromElements(dummy).returns(ExternalTypeInfo.<Row>of(input));
    var schema = Schema.newBuilder().fromRowDataType(input);
    if ((isWindow(name) || name.startsWith("attached-")))
      schema.watermark("rt", "rt - INTERVAL '1' SECOND");
    if (name.startsWith("over-map-")) schema.columnByExpression("pt", "PROCTIME()");
    table.createTemporaryView("src", table.fromDataStream(source, schema.build()));
    String sql;
    if (name.startsWith("over-map-")) {
      String frame =
          name.endsWith("bounded")
              ? "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
              : name.endsWith("range") ? "RANGE UNBOUNDED PRECEDING" : "ROWS UNBOUNDED PRECEDING";
      sql = "SELECT k, COUNT(m) OVER (PARTITION BY k ORDER BY pt " + frame + ") FROM src";
    } else if (name.startsWith("shared-")) {
      table.createTemporarySystemFunction(
          "tick",
          name.equals("shared-int")
              ? new SharedStatefulScalarBenchmark.IntTick()
              : name.equals("shared-long")
                  ? new SharedStatefulScalarBenchmark.LongTick()
                  : new SharedStatefulScalarBenchmark.TextTick());
      sql = "SELECT id, tick(id), tick(id + 1) FROM src";
    } else if (name.startsWith("temporal-")) {
      table.createTemporarySystemFunction(
          "ts_identity", new FlinkTemporalUdfSqlHarnessTest.TimestampIdentity(9));
      table.createTemporarySystemFunction(
          "ltz_identity", new FlinkTemporalUdfSqlHarnessTest.LtzIdentity(9));
      table.createTemporarySystemFunction(
          "ts_text", new FlinkTemporalUdfSqlHarnessTest.TimestampText(9));
      table.createTemporarySystemFunction(
          "ltz_epoch", new FlinkTemporalUdfSqlHarnessTest.LtzEpoch(9, false));
      sql =
          name.equals("temporal-identity")
              ? "SELECT id, ts_identity(t), ltz_identity(z) FROM src"
              : name.equals("temporal-text")
                  ? "SELECT id, ts_text(t), z FROM src"
                  : "SELECT id, t, ltz_epoch(z) FROM src";
    } else if (name.startsWith("attached-")) {
      String aggregate = name.equals("attached-sum") ? "SUM(v)" : "COUNT(*)";
      int size = name.equals("attached-count-wide") ? 30 : 10;
      sql =
          "SELECT MAX(n), wstart, wend FROM (SELECT k, "
              + aggregate
              + " AS n, window_start AS wstart, window_end AS wend FROM TABLE(HOP(TABLE src,"
              + " DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '"
              + size
              + "' SECOND)) GROUP BY k, window_start, window_end) counts GROUP BY wstart, wend";
    } else if (name.equals("string-session")) {
      sql =
          "SELECT k, MIN(s), MAX(s), COUNT(*), "
              + "SESSION_START(rt, INTERVAL '2' SECOND), SESSION_END(rt, INTERVAL '2' SECOND) "
              + "FROM src GROUP BY k, SESSION(rt, INTERVAL '2' SECOND)";
    } else {
      String window = name.substring(name.indexOf('-') + 1).toUpperCase(Locale.ROOT);
      String bounds =
          window.equals("TUMBLE")
              ? ", INTERVAL '10' SECOND"
              : ", INTERVAL '2' SECOND, INTERVAL '10' SECOND";
      sql =
          "SELECT k, "
              + (name.startsWith("count-")
                  ? "COUNT(b), COUNT(d), COUNT(m)"
                  : "MIN(s), MAX(s), COUNT(*)")
              + ", window_start, window_end FROM TABLE("
              + window
              + "(TABLE src, DESCRIPTOR(rt)"
              + bounds
              + ")) GROUP BY k, window_start, window_end";
    }
    if (nativeEngine) NativePlanner.install(table);
    var query = table.sqlQuery(sql);
    var pending = new ArrayDeque<Transformation<?>>();
    var seen = new ArrayList<String>();
    pending.add(table.toChangelogStream(query).getTransformation());
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      seen.add(node.getName());
      if (node instanceof OneInputTransformation<?, ?>) {
        var one = (OneInputTransformation<?, ?>) node;
        boolean match =
            name.startsWith("over-map-")
                ? node.getName().contains("OverAggregate")
                : (isWindow(name) || name.startsWith("attached-"))
                    ? node.getName()
                        .contains(
                            nativeEngine
                                ? name.equals("string-session")
                                    ? "NativeColumnarSessionWindowAgg"
                                    : "NativeColumnarWindowAgg"
                                : name.equals("string-session")
                                    ? "GroupWindowAggregate"
                                    : "WindowAggregate")
                    : node.getName().contains(nativeEngine ? "NativeCalc" : "Calc")
                        && (nativeEngine
                            || one.getInputs().get(0).getOutputType() instanceof InternalTypeInfo);
        if (match) {
          RowType output =
              (RowType) query.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
          if (!nativeEngine) output = ((InternalTypeInfo<?>) node.getOutputType()).toRowType();
          if (nativeEngine && (isWindow(name) || name.startsWith("attached-"))) {
            var factory = (SimpleOperatorFactory<?>) one.getOperatorFactory();
            Field field = NativeRowWindowOperatorCore.class.getDeclaredField("outputType");
            field.setAccessible(true);
            output = (RowType) field.get(factory.getOperator());
          }
          // Legacy group windows append unrequested rowtime/proctime properties.
          // Consume the six SQL output fields on both engines.
          if (name.equals("string-session")) output = new RowType(output.getFields().subList(0, 6));
          RowType physicalInput =
              nativeEngine
                  ? suppliedInput
                  : ((InternalTypeInfo<?>) one.getInputs().get(0).getOutputType()).toRowType();
          return new Plan(nativeEngine, physicalInput, output, one.getOperatorFactory(), one);
        }
      }
      pending.addAll(node.getInputs());
    }
    throw new AssertionError(
        "production operator missing: " + name + ", native=" + nativeEngine + ", nodes=" + seen);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static OneInputStreamOperatorTestHarness<Object, Object> harness(Plan plan, Digest digest)
      throws Exception {
    StreamOperatorFactory factory =
        InstantiationUtil.deserializeObject(
            plan.factory, FeatureAdmissionBenchmark.class.getClassLoader());
    OneInputStreamOperatorTestHarness harness;
    if (plan.transformation != null && plan.transformation.getStateKeySelector() != null) {
      harness =
          new KeyedOneInputStreamOperatorTestHarness(
              factory,
              plan.transformation.getStateKeySelector(),
              plan.transformation.getStateKeyType(),
              128,
              1,
              0);
    } else harness = new OneInputStreamOperatorTestHarness(factory, 128, 1, 0);
    String backend = System.getProperty("admission.backend", "memory");
    if (backend.equals("rocksdb")) {
      var config = new Configuration();
      config.setString("state.backend.rocksdb.memory.fixed-per-slot", "64 mb");
      harness.setStateBackend(
          new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
              .createFromConfig(config, FeatureAdmissionBenchmark.class.getClassLoader()));
    } else if (!backend.equals("memory")) throw new IllegalArgumentException(backend);
    harness.setup(
        plan.nativeEngine ? new ArrowBatchSerializer() : new RowDataSerializer(plan.output));
    Field output = AbstractStreamOperator.class.getDeclaredField("output");
    output.setAccessible(true);
    output.set(
        harness.getOperator(),
        new FlinkOutput<Object>(null) {
          @Override
          public void collect(StreamRecord<Object> record) {
            if (plan.nativeEngine) {
              try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
                var reader = ArrowConversion.createArrowReader(root, plan.output);
                var kinds =
                    (org.apache.arrow.vector.TinyIntVector)
                        root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
                for (int index = 0; index < root.getRowCount(); index++) {
                  RowData row = reader.read(index);
                  row.setRowKind(
                      kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(index)));
                  digest.add(row, root, index);
                }
              }
            } else digest.add((RowData) record.getValue(), null, -1);
          }

          @Override
          public void emitWatermark(Watermark watermark) {}
        });
    harness.open();
    return harness;
  }

  private static DataType inputType(String name) {
    if (name.startsWith("over-map-"))
      return DataTypes.ROW(
          DataTypes.FIELD("k", DataTypes.BIGINT()),
          DataTypes.FIELD("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
    if (name.startsWith("count-"))
      return DataTypes.ROW(
          DataTypes.FIELD("k", DataTypes.BIGINT()),
          DataTypes.FIELD("b", DataTypes.BYTES()),
          DataTypes.FIELD("d", DataTypes.INTERVAL(DataTypes.DAY(6), DataTypes.SECOND(3))),
          DataTypes.FIELD("m", DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH())),
          DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    if (name.startsWith("attached-"))
      return DataTypes.ROW(
          DataTypes.FIELD("k", DataTypes.INT()),
          DataTypes.FIELD("v", DataTypes.BIGINT()),
          DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    if (name.equals("drop"))
      return DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.BIGINT()), DataTypes.FIELD("s", DataTypes.STRING()));
    if ((isWindow(name) || name.startsWith("attached-")))
      return DataTypes.ROW(
          DataTypes.FIELD("k", DataTypes.BIGINT()),
          DataTypes.FIELD("s", DataTypes.STRING()),
          DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    if (name.startsWith("temporal-"))
      return DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.INT()),
          DataTypes.FIELD("t", DataTypes.TIMESTAMP(9)),
          DataTypes.FIELD("z", DataTypes.TIMESTAMP_LTZ(9)));
    return DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
  }

  private static List<RowData> rows(
      String name, int count, int keys, int width, RowType inputType) {
    var rows = new ArrayList<RowData>(count);
    StringData[] strings = new StringData[1024];
    for (int i = 0; i < strings.length; i++)
      strings[i] =
          StringData.fromString(
              String.format(Locale.ROOT, "%08d", i) + "x".repeat(Math.max(0, width - 8)));
    for (int i = 0; i < count; i++) {
      boolean missing = i % 7 == 0;
      GenericRowData row;
      if (name.startsWith("over-map-")) {
        var map = new LinkedHashMap<StringData, Long>();
        if (i % 3 != 0)
          for (int element = 0; element < width; element++)
            map.put(
                StringData.fromString("key" + element), element % 3 == 0 ? null : (long) (i % 127));
        row = new GenericRowData(inputType.getFieldCount());
        for (int field = 0; field < inputType.getFieldCount(); field++) {
          switch (inputType.getTypeAt(field).getTypeRoot()) {
            case BIGINT:
              row.setField(field, (long) (i % keys));
              break;
            case MAP:
              row.setField(field, missing ? null : new GenericMapData(map));
              break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              row.setField(field, TimestampData.fromEpochMillis(0));
              break;
            default:
              throw new AssertionError(inputType);
          }
        }
      } else if (name.startsWith("count-")) {
        byte[] bytes = new byte[width];
        java.util.Arrays.fill(bytes, (byte) 0xff);
        row =
            GenericRowData.of(
                i % 11 == 0 ? null : (long) (i % keys),
                missing ? null : i % 5 == 0 ? new byte[0] : bytes,
                i % 5 == 0 ? null : (i % 1000 - 500L) * 1234,
                i % 3 == 0 ? null : i % 1000 - 500,
                TimestampData.fromEpochMillis(i % 10000));
      } else if (name.startsWith("attached-")) {
        row = new GenericRowData(inputType.getFieldCount());
        long start = 1577836800000L + (i % 25) * 2000;
        long end = start + (name.equals("attached-count-wide") ? 30000 : 10000);
        for (int field = 0; field < inputType.getFieldCount(); field++) {
          var type = inputType.getTypeAt(field);
          String fieldName = inputType.getFieldNames().get(field);
          switch (type.getTypeRoot()) {
            case BIGINT:
              row.setField(
                  field, name.equals("attached-sum") && missing ? null : (long) (i % 1000));
              break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              row.setField(
                  field,
                  TimestampData.fromEpochMillis(
                      fieldName.contains("start")
                          ? start
                          : fieldName.contains("end") ? end : end - 1));
              break;
            default:
              throw new AssertionError(inputType);
          }
        }
      } else if (name.equals("drop")) {
        row = GenericRowData.of((long) i, missing ? null : strings[i % strings.length]);
        row.setRowKind(RowKind.values()[i % 4]);
      } else if ((isWindow(name) || name.startsWith("attached-")))
        row =
            GenericRowData.of(
                i % 11 == 0 ? null : (long) (i % keys),
                missing ? null : strings[(i + i / keys) % strings.length],
                TimestampData.fromEpochMillis(i % 10000));
      else if (name.startsWith("temporal-")) {
        var timestamp =
            missing ? null : TimestampData.fromEpochMillis(1577836800000L + i % 10000, i % 1000000);
        row = GenericRowData.of(i, timestamp, timestamp);
      } else row = GenericRowData.of(missing ? null : i);
      rows.add(row);
    }
    return rows;
  }

  private static final class Digest {
    final RowType type;
    final Map<String, Long> values;
    final int fixedSchema;
    long count;
    long hash;

    Digest(RowType type, boolean capture) {
      this.type = type;
      values = capture ? new TreeMap<>() : null;
      var roots =
          type.getChildren().stream()
              .map(org.apache.flink.table.types.logical.LogicalType::getTypeRoot)
              .collect(java.util.stream.Collectors.toList());
      var bigint = org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
      var varchar = org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;
      var timestamp =
          org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
      fixedSchema =
          roots.equals(List.of(bigint, varchar))
              ? 1
              : roots.equals(List.of(bigint, bigint, bigint, bigint, timestamp, timestamp))
                  ? 3
                  : roots.equals(List.of(bigint, varchar, varchar, bigint, timestamp, timestamp))
                          && org.apache.flink.table.types.logical.utils.LogicalTypeChecks
                                  .getPrecision(type.getTypeAt(4))
                              == 3
                          && org.apache.flink.table.types.logical.utils.LogicalTypeChecks
                                  .getPrecision(type.getTypeAt(5))
                              == 3
                      ? 2
                      : 0;
    }

    private static long stringBytes(RowData row, VectorSchemaRoot root, int index, int field) {
      if (row.isNullAt(field)) return 0;
      return root == null
          ? ((BinaryStringData) row.getString(field)).getSizeInBytes()
          : ((VarCharVector) root.getVector(field)).getValueLength(index);
    }

    void add(RowData row, VectorSchemaRoot root, int index) {
      count++;
      long h = row.getRowKind().toByteValue();
      if (values == null && fixedSchema == 3) {
        for (int field = 0; field < 4; field++)
          h = h * 31 + (row.isNullAt(field) ? 0 : row.getLong(field));
        h = h * 31 + row.getTimestamp(4, 3).getMillisecond() * 31;
        h = h * 31 + row.getTimestamp(5, 3).getMillisecond() * 31;
        hash += h;
        return;
      }
      if (values == null && fixedSchema != 0) {
        h = h * 31 + (row.isNullAt(0) ? 0 : row.getLong(0));
        h = h * 31 + stringBytes(row, root, index, 1);
        if (fixedSchema == 2) {
          h = h * 31 + stringBytes(row, root, index, 2);
          h = h * 31 + row.getLong(3);
          h = h * 31 + row.getTimestamp(4, 3).getMillisecond() * 31;
          h = h * 31 + row.getTimestamp(5, 3).getMillisecond() * 31;
        }
        hash += h;
        return;
      }
      StringBuilder text =
          values == null ? null : new StringBuilder(row.getRowKind().shortString());
      for (int field = 0; field < type.getFieldCount(); field++) {
        Object value = null;
        long v = 0;
        if (!row.isNullAt(field)) {
          switch (type.getTypeAt(field).getTypeRoot()) {
            case INTEGER:
              v = row.getInt(field);
              if (text != null) value = v;
              break;
            case BIGINT:
              v = row.getLong(field);
              if (text != null) value = v;
              break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              var timestamp =
                  row.getTimestamp(
                      field,
                      org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(
                          type.getTypeAt(field)));
              v = timestamp.getMillisecond() * 31 + timestamp.getNanoOfMillisecond();
              if (text != null) value = timestamp;
              break;
            case MAP:
              var map = row.getMap(field);
              v = map.size();
              if (text != null) {
                var entries = new TreeMap<String, Long>();
                for (int element = 0; element < map.size(); element++)
                  entries.put(
                      map.keyArray().getString(element).toString(),
                      map.valueArray().isNullAt(element)
                          ? null
                          : map.valueArray().getLong(element));
                value = entries;
              }
              break;
            case VARCHAR:
            case CHAR:
              v =
                  root == null
                      ? ((BinaryStringData) row.getString(field)).getSizeInBytes()
                      : ((VarCharVector) root.getVector(field)).getValueLength(index);
              if (text != null) value = row.getString(field).toString();
              break;
            default:
              throw new AssertionError(type.getTypeAt(field));
          }
        }
        h = h * 31 + v;
        if (text != null) text.append('|').append(value);
      }
      hash += h;
      if (text != null) values.merge(text.toString(), 1L, Long::sum);
    }
  }

  @SuppressWarnings("unchecked")
  private static int keyGroup(Plan stock, RowData row) throws Exception {
    if (stock.transformation == null || stock.transformation.getStateKeySelector() == null)
      return -1;
    var selector = (KeySelector<RowData, Object>) stock.transformation.getStateKeySelector();
    return org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignToKeyGroup(
        selector.getKey(row), 128);
  }

  private static double median(double[] values) {
    double[] copy = values.clone();
    Arrays.sort(copy);
    return copy[copy.length / 2];
  }
}
