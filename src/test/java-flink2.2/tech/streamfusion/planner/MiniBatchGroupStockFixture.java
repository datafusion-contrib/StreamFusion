package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.*;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.*;
import org.apache.flink.table.runtime.operators.bundle.AbstractMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.*;
import tech.streamfusion.state.*;

final class MiniBatchGroupStockFixture implements AutoCloseable {
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
  final RowDataKeySelector selector;
  final RowType inputType, outputType, accumulatorType, seedType;
  final Object function;
  final StockGroupTypedStateContract contract;
  final NativeColumnarGroupAggregateOperator template;
  final Object seedTemplate;
  final OneInputStreamOperatorTestHarness<RowData, RowData> local;
  final boolean rocks, global;

  MiniBatchGroupStockFixture(String type, boolean global, boolean rocks) throws Exception {
    this(type, global, rocks, false);
  }

  @SuppressWarnings("unchecked")
  MiniBatchGroupStockFixture(String type, boolean global, boolean rocks, boolean distinct)
      throws Exception {
    this.rocks = rocks;
    this.global = global;
    var stock = graph(type, global, false, distinct);
    StreamNode node =
        stock.getStreamNodes().stream()
            .filter(
                n -> {
                  Object op = operator(n);
                  if (!(op instanceof KeyedMapBundleOperator)) return false;
                  try {
                    return field(op, "function").getClass()
                        == (global
                            ? MiniBatchGlobalGroupAggFunction.class
                            : MiniBatchGroupAggFunction.class);
                  } catch (Exception failure) {
                    throw new RuntimeException(failure);
                  }
                })
            .findFirst()
            .orElseThrow();
    var op = (OneInputStreamOperator<RowData, RowData>) operator(node);
    function = field(op, "function");
    selector = (RowDataKeySelector) node.getStatePartitioners()[0];
    inputType = RowType.of((LogicalType[]) field(node.getTypeSerializersIn()[0], "types"));
    outputType = RowType.of((LogicalType[]) field(node.getTypeSerializerOut(), "types"));
    accumulatorType = RowType.of((LogicalType[]) field(function, "accTypes"));
    harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            op, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
    harness.setup((TypeSerializer<RowData>) node.getTypeSerializerOut());
    if (global) {
      var localNode =
          stock.getStreamNodes().stream()
              .filter(
                  n -> {
                    Object localOp = operator(n);
                    if (!(localOp instanceof AbstractMapBundleOperator)) return false;
                    try {
                      return field(localOp, "function") instanceof MiniBatchLocalGroupAggFunction;
                    } catch (Exception failure) {
                      throw new RuntimeException(failure);
                    }
                  })
              .findFirst()
              .orElseThrow();
      local =
          new OneInputStreamOperatorTestHarness<>(
              (OneInputStreamOperator<RowData, RowData>) operator(localNode));
      local.setup((TypeSerializer<RowData>) localNode.getTypeSerializerOut());
      seedType = RowType.of((LogicalType[]) field(localNode.getTypeSerializersIn()[0], "types"));
    } else {
      local = null;
      seedType = inputType;
    }
    var nativeGraph = graph(type, global, true, distinct);
    template =
        (NativeColumnarGroupAggregateOperator)
            nativeGraph.getStreamNodes().stream()
                .map(MiniBatchGroupStockFixture::operator)
                .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new AssertionError(nativeGraph.getStreamingPlanAsJSON()));
    seedTemplate =
        global
            ? nativeGraph.getStreamNodes().stream()
                .map(MiniBatchGroupStockFixture::operator)
                .filter(NativeColumnarLocalGroupAggregateOperator.class::isInstance)
                .findFirst()
                .orElseThrow()
            : template;
    contract =
        assertInstanceOf(StockGroupTypedStateContract.class, field(template, "stockStateContract"));
    assertEquals(accumulatorType, field(contract, "accumulatorType"));
    assertEquals(global, field(contract, "globalMerge"));
  }

  private static Object operator(StreamNode node) {
    return node.getOperatorFactory() instanceof SimpleOperatorFactory
        ? ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
        : null;
  }

  static java.util.stream.Stream<String> types() {
    return java.util.stream.Stream.of(
        "STRING",
        "VARCHAR(4096)",
        "CHAR(4)",
        "BIGINT",
        "INT",
        "SMALLINT",
        "TINYINT",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME(0)",
        "TIME(3)",
        "BOOLEAN");
  }

  static String arithmeticType(String type) {
    return java.util.Set.of("BIGINT", "INT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE")
            .contains(type)
        ? type
        : "INT";
  }

  static StreamGraph graph(String type, boolean global, boolean nativeEnabled) throws Exception {
    return graph(type, global, nativeEnabled, false);
  }

  static StreamGraph graph(String type, boolean global, boolean nativeEnabled, boolean distinct)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    config.setString("pipeline.max-parallelism", "128");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "5");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "10 s");
    table.getConfig().set("table.optimizer.agg-phase-strategy", global ? "TWO_PHASE" : "ONE_PHASE");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,a "
            + arithmeticType(type)
            + ",v "
            + (distinct ? "STRING" : type)
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='true')");
    table.useCatalog("default_catalog");
    String aggregates =
        distinct
            ? "sd "
                + type
                + ",sf "
                + type
                + ",dn BIGINT,df BIGINT,av "
                + (type.equals("FLOAT") || type.equals("DOUBLE") ? "DOUBLE" : "BIGINT")
                + ",ds BIGINT"
            : "av "
                + arithmeticType(type)
                + ",lo "
                + type
                + ",hi "
                + type
                + ",s "
                + arithmeticType(type);
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,n BIGINT,"
            + aggregates
            + ") WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    return CanonicalRegularJoinPlanTest.graph(
        planner,
        table,
        CanonicalRegularJoinPlanTest.translate(
            planner,
            distinct
                ? "INSERT INTO output_table SELECT k,COUNT(*),SUM(DISTINCT a),SUM(DISTINCT a)"
                      + " FILTER(WHERE a>1),COUNT(DISTINCT a),COUNT(DISTINCT a) FILTER(WHERE"
                      + " a>1),AVG(a),COUNT(DISTINCT v) FROM fixture.db.input_table GROUP BY k"
                : "INSERT INTO output_table SELECT k,COUNT(*),AVG(a) FILTER(WHERE"
                      + " a>1),MIN(v),MAX(v),SUM(a) FILTER(WHERE a>1) FROM fixture.db.input_table"
                      + " GROUP BY k"));
  }

  RowData seed(Long key, Integer value, String text, boolean javaCache) throws Exception {
    var row = new GenericRowData(seedType.getFieldCount());
    for (int column : (int[]) field(seedTemplate, "keyColumns")) row.setField(column, key);
    int[] columns = (int[]) field(seedTemplate, "valueColumns"),
        kinds = (int[]) field(seedTemplate, "aggregateKinds");
    for (int slot = 0; slot < columns.length; slot++)
      if (columns[slot] >= 0) {
        boolean extreme = kinds[slot] == 10 || kinds[slot] == 11;
        row.setField(
            columns[slot],
            seedValue(
                seedType.getTypeAt(columns[slot]),
                extreme && text == null ? null : value,
                text,
                javaCache));
      }
    for (int filter : (int[]) field(seedTemplate, "filterColumns"))
      if (filter >= 0) row.setField(filter, value != null && value > 1);
    return row;
  }

  private static Object seedValue(LogicalType type, Integer value, String text, boolean javaCache) {
    switch (type.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return text == null
            ? null
            : javaCache
                ? StringData.fromString(text)
                : BinaryStringData.fromBytes(
                    text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      case BIGINT:
        return value == null ? null : value.longValue();
      case SMALLINT:
        return value == null ? null : value.shortValue();
      case TINYINT:
        return value == null ? null : value.byteValue();
      case FLOAT:
        return value == null ? null : value.floatValue() + 0.25f;
      case DOUBLE:
        return value == null ? null : value.doubleValue() + 0.5;
      case BOOLEAN:
        return value == null ? null : value % 2 != 0;
      case TIME_WITHOUT_TIME_ZONE:
        return value == null ? null : Math.floorMod(value, 60) * 1000;
      case DATE:
      case INTEGER:
        return value;
      default:
        throw new AssertionError(type);
    }
  }

  void openStock(OperatorSubtaskState saved) throws Exception {
    if (saved != null) harness.initializeState(saved);
    harness.open();
    if (local != null) local.open();
  }

  List<RowData> partials(List<RowData> rows) throws Exception {
    if (local == null) return rows;
    local.getOutput().clear();
    for (var row : rows)
      local.processElement(new StreamRecord<>(new RowDataSerializer(seedType).copy(row)));
    ((AbstractMapBundleOperator<?, ?, ?, ?>) local.getOperator()).finishBundle();
    var out = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(inputType);
    for (var row : local.extractOutputValues()) out.add(serializer.copy(row));
    return out;
  }

  List<String> stockBundle(List<RowData> rows) throws Exception {
    harness.getOutput().clear();
    for (var row : rows)
      harness.processElement(new StreamRecord<>(new RowDataSerializer(inputType).copy(row)));
    ((AbstractMapBundleOperator<?, ?, ?, ?>) harness.getOperator()).finishBundle();
    return bytes(harness.extractOutputValues(), outputType);
  }

  @SuppressWarnings("unchecked")
  Map<String, String> rawStateBytes() throws Exception {
    var backend = (CheckpointableKeyedStateBackend<?>) harness.getOperator().getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable ignored = resources::release) {
      var result = new TreeMap<String, String>();
      var metadata = resources.getMetaInfoSnapshots();
      try (var iterator = resources.createKVStateIterator()) {
        while (iterator.isValid()) {
          result.put(
              metadata.get(iterator.kvStateId()).getName()
                  + ":"
                  + Base64.getEncoder().encodeToString(iterator.key()),
              Base64.getEncoder().encodeToString(iterator.value()));
          iterator.next();
        }
      }
      return result;
    }
  }

  static List<String> bytes(Iterable<RowData> rows, RowType type) throws Exception {
    var grouped = new TreeMap<String, List<String>>();
    var serializer = new RowDataSerializer(type);
    for (var row : rows) {
      var bytes = new org.apache.flink.core.memory.DataOutputSerializer(64);
      serializer.serialize(row, bytes);
      String key = row.isNullAt(0) ? "null" : Long.toString(row.getLong(0));
      grouped
          .computeIfAbsent(key, ignored -> new ArrayList<>())
          .add(Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
    }
    var out = new ArrayList<String>();
    grouped.values().forEach(out::addAll);
    return out;
  }

  KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness()
      throws Exception {
    var owner = new Direct(this);
    owner.setStockStateContract(contract);
    var target =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            owner, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        target.getStreamConfig().getOperatorID().toHexString(),
        owner.getClass().getSimpleName(),
        contract,
        getClass().getClassLoader());
    target.setStateBackend(
        new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
    target.setup(new ArrowBatchSerializer());
    return target;
  }

  OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> nativeLocalHarness() throws Exception {
    assertTrue(global);
    var owner =
        new NativeColumnarLocalGroupAggregateOperator(
            (int[]) field(seedTemplate, "aggregateKinds"),
            (int[]) field(seedTemplate, "valueTypes"),
            (int[]) field(seedTemplate, "valueColumns"),
            (int[]) field(seedTemplate, "filterColumns"),
            (int[]) field(seedTemplate, "keyColumns"),
            (int[]) field(seedTemplate, "distinctViewSources"),
            (int[]) field(seedTemplate, "keyTimestampPrecisions"),
            (long) field(seedTemplate, "miniBatchSize"),
            (int[]) field(seedTemplate, "auxiliaryColumns"));
    if ((boolean) field(seedTemplate, "stockAppendDistinct")) owner.withStockAppendDistinct();
    var target = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(owner);
    target.setup(new ArrowBatchSerializer());
    return target;
  }

  static final class Direct extends NativeColumnarGroupAggregateOperator {
    boolean checkpoint;

    Direct(MiniBatchGroupStockFixture fixture) throws Exception {
      super(
          (int[]) field(fixture.template, "aggregateKinds"),
          (int[]) field(fixture.template, "valueTypes"),
          (int[]) field(fixture.template, "valueColumns"),
          (int[]) field(fixture.template, "keyColumns"),
          (int[]) field(fixture.template, "filterColumns"),
          (int[]) field(fixture.template, "countColumns"),
          (int[]) field(fixture.template, "distinctViewColumns"),
          (int) field(fixture.template, "recordCountColumn"),
          (boolean) field(fixture.template, "generateUpdateBefore"),
          true,
          (long) field(fixture.template, "miniBatchSize"),
          (long) field(fixture.template, "stateTtlMillis"),
          FlinkKeyGroupUtils.stockKeyDescriptor(
              fixture.inputType, (int[]) field(fixture.template, "keyColumns")),
          128);
    }

    long address() {
      return handle;
    }

    boolean persistent() {
      return directRocksDBState();
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      if (checkpoint) return super.snapshotRawPartitions();
      throw new AssertionError("canonical used native snapshot");
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (checkpoint) return super.snapshotCanonicalPartitions();
      throw new AssertionError("canonical used native partitions");
    }
  }

  @Override
  public void close() throws Exception {
    harness.close();
    if (local != null) local.close();
  }
}
