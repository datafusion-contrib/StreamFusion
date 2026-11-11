package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.*;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.*;

final class TypedGroupStockFixture implements AutoCloseable {
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
  final RowDataKeySelector selector;
  final RowType inputType, outputType, accumulatorType;
  final Object function;
  final StockGroupTypedStateContract contract;
  final NativeColumnarGroupAggregateOperator template;
  final boolean rocks, miniBatch;
  final int[] logicalColumns;

  TypedGroupStockFixture(String numeric, boolean append, boolean rocks) throws Exception {
    this(numeric, append, rocks, false);
  }

  @SuppressWarnings("unchecked")
  TypedGroupStockFixture(String numeric, boolean append, boolean rocks, boolean extremaOnly)
      throws Exception {
    this(numeric, append, rocks, extremaOnly, false, false);
  }

  @SuppressWarnings("unchecked")
  TypedGroupStockFixture(
      String numeric,
      boolean append,
      boolean rocks,
      boolean extremaOnly,
      boolean distinctSum,
      boolean miniBatch)
      throws Exception {
    this.rocks = rocks;
    this.miniBatch = miniBatch;
    var graph = graph(numeric, append, false, extremaOnly, distinctSum, miniBatch);
    var node =
        graph.getStreamNodes().stream()
            .filter(
                n -> {
                  if (!(n.getOperatorFactory() instanceof SimpleOperatorFactory)) return false;
                  var op = ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator();
                  if (op instanceof KeyedProcessOperator)
                    return ((KeyedProcessOperator<?, ?, ?>) op).getUserFunction()
                        instanceof GroupAggFunction;
                  if (op
                      instanceof
                      org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator)
                    try {
                      return field(op, "function")
                          instanceof
                          org.apache.flink.table.runtime.operators.aggregate
                              .MiniBatchGroupAggFunction;
                    } catch (Exception failure) {
                      throw new RuntimeException(failure);
                    }
                  return false;
                })
            .findFirst()
            .orElseThrow();
    var operator =
        (OneInputStreamOperator<RowData, RowData>)
            ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    function =
        operator instanceof KeyedProcessOperator
            ? ((KeyedProcessOperator<?, ?, ?>) operator).getUserFunction()
            : field(operator, "function");
    selector = (RowDataKeySelector) node.getStatePartitioners()[0];
    inputType = RowType.of((LogicalType[]) field(node.getTypeSerializersIn()[0], "types"));
    outputType = RowType.of((LogicalType[]) field(node.getTypeSerializerOut(), "types"));
    accumulatorType = RowType.of((LogicalType[]) field(function, "accTypes"));
    assertEquals(
        5,
        inputType.getFieldCount(),
        "physical input retains both keys, numeric, member and filter");
    assertTrue(inputType.getTypeAt(0).is(LogicalTypeRoot.VARCHAR));
    assertTrue(inputType.getTypeAt(1).is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE));
    logicalColumns = new int[] {0, 1, 2, 3, 4};
    if (distinctSum) {
      for (int i = 2; i < inputType.getFieldCount(); i++) {
        if (inputType.getTypeAt(i).is(LogicalTypeRoot.VARCHAR)) logicalColumns[3] = i;
        else if (inputType.getTypeAt(i).is(LogicalTypeRoot.BOOLEAN)) logicalColumns[4] = i;
        else logicalColumns[2] = i;
      }
      assertEquals(5, Arrays.stream(logicalColumns).distinct().count());
    }
    assertTrue(inputType.getTypeAt(logicalColumns[3]).is(LogicalTypeRoot.VARCHAR));
    assertTrue(inputType.getTypeAt(logicalColumns[4]).is(LogicalTypeRoot.BOOLEAN));
    harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    harness.setup(
        (org.apache.flink.api.common.typeutils.TypeSerializer<RowData>)
            node.getTypeSerializerOut());
    var nativeGraph = graph(numeric, append, true, extremaOnly, distinctSum, miniBatch);
    template =
        (NativeColumnarGroupAggregateOperator)
            nativeGraph.getStreamNodes().stream()
                .filter(n -> n.getOperatorFactory() instanceof SimpleOperatorFactory)
                .map(n -> ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator())
                .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new AssertionError(nativeGraph.getStreamingPlanAsJSON()));
    contract =
        assertInstanceOf(StockGroupTypedStateContract.class, field(template, "stockStateContract"));
    assertEquals(accumulatorType, field(contract, "accumulatorType"));
    assertEquals(2, ((RowType) field(contract, "keyType")).getFieldCount());
    var views = (List<?>) field(contract, "views");
    assertEquals(distinctSum ? 2 : 1, views.size());
    if (distinctSum) {
      var widths = new ArrayList<Integer>();
      for (var view : views) widths.add(((int[]) field(view, "aggregates")).length);
      Collections.sort(widths);
      assertEquals(List.of(1, 4), widths);
    } else assertEquals(2, ((int[]) field(views.get(0), "aggregates")).length);
  }

  private static StreamGraph graph(
      String numeric, boolean append, boolean nativeEnabled, boolean extremaOnly) throws Exception {
    return graph(numeric, append, nativeEnabled, extremaOnly, false, false);
  }

  private static StreamGraph graph(
      String numeric,
      boolean append,
      boolean nativeEnabled,
      boolean extremaOnly,
      boolean distinctSum,
      boolean miniBatch)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    config.setString("pipeline.max-parallelism", "128");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.size", "5");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    }
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
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
        "CREATE TABLE input_table(k STRING,t TIMESTAMP(3),v "
            + numeric
            + ",s STRING,f BOOLEAN) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='"
            + append
            + "')");
    table.useCatalog("default_catalog");
    String average = List.of("FLOAT", "DOUBLE").contains(numeric) ? "DOUBLE" : "BIGINT";
    String aggregates =
        distinctSum
            ? "sd "
                + numeric
                + ",sf "
                + numeric
                + ",dn BIGINT,df BIGINT,av "
                + average
                + ",ds BIGINT"
            : extremaOnly
                ? "lo " + numeric + ",hi " + numeric + ",d BIGINT,e BIGINT"
                : "total "
                    + numeric
                    + ",a "
                    + average
                    + ",d BIGINT,e BIGINT"
                    + (append ? ",lo " + numeric + ",hi " + numeric : "");
    table.executeSql(
        "CREATE TABLE output_table(k STRING,t TIMESTAMP(3),n BIGINT,"
            + aggregates
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,t,COUNT(*),"
            + (distinctSum
                ? "SUM(DISTINCT v),SUM(DISTINCT v) FILTER(WHERE f),COUNT(DISTINCT v),"
                    + "COUNT(DISTINCT v) FILTER(WHERE f),AVG(v),COUNT(DISTINCT s)"
                : (extremaOnly ? "MIN(v),MAX(v)" : "SUM(v),AVG(v)")
                    + ",COUNT(DISTINCT s),COUNT(DISTINCT s) FILTER(WHERE f)"
                    + (append && !extremaOnly ? ",MIN(v),MAX(v)" : ""))
            + " FROM fixture.db.input_table GROUP BY k,t";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    return CanonicalRegularJoinPlanTest.graph(
        planner, table, CanonicalRegularJoinPlanTest.translate(planner, query));
  }

  RowData physicalRow(RowData logical) {
    var physical = new org.apache.flink.table.data.GenericRowData(inputType.getFieldCount());
    physical.setRowKind(logical.getRowKind());
    for (int i = 0; i < logicalColumns.length; i++) {
      var type = inputType.getTypeAt(logicalColumns[i]);
      // 1.18 materializes FILTER's nullable flag as a non-null IS TRUE expression.
      Object value =
          type.is(LogicalTypeRoot.BOOLEAN) && !type.isNullable() && logical.isNullAt(i)
              ? Boolean.FALSE
              : RowData.createFieldGetter(type.copy(true), i).getFieldOrNull(logical);
      physical.setField(logicalColumns[i], value);
    }
    return physical;
  }

  @SuppressWarnings("unchecked")
  CheckpointableKeyedStateBackend<RowData> backend() {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  Map<String, String> rawStateBytes() throws Exception {
    var resources = backend().savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
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

  KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness()
      throws Exception {
    var operator = new Direct(template, inputType);
    operator.setStockStateContract(contract);
    var nativeHarness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, b -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        nativeHarness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        getClass().getClassLoader());
    nativeHarness.setStateBackend(
        new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
    nativeHarness.setup(new ArrowBatchSerializer());
    return nativeHarness;
  }

  static final class Direct extends NativeColumnarGroupAggregateOperator {
    boolean checkpoint;

    Direct(NativeColumnarGroupAggregateOperator source, RowType input) throws Exception {
      super(
          (int[]) field(source, "aggregateKinds"),
          (int[]) field(source, "valueTypes"),
          (int[]) field(source, "valueColumns"),
          (int[]) field(source, "keyColumns"),
          (int[]) field(source, "filterColumns"),
          (int[]) field(source, "countColumns"),
          (int[]) field(source, "distinctViewColumns"),
          (int) field(source, "recordCountColumn"),
          (boolean) field(source, "generateUpdateBefore"),
          (boolean) field(source, "miniBatch"),
          (long) field(source, "miniBatchSize"),
          (long) field(source, "stateTtlMillis"),
          FlinkKeyGroupUtils.stockKeyDescriptor(input, (int[]) field(source, "keyColumns")),
          128);
    }

    long address() {
      return handle;
    }

    boolean persistent() {
      return directRocksDBState();
    }

    void assertStorage(boolean rocks) {
      assertEquals(rocks, rocksdbState());
      assertEquals(rocks, directRocksDBState());
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      if (checkpoint) return super.snapshotRawPartitions();
      throw new AssertionError("canonical export used a native snapshot");
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (checkpoint) return super.snapshotCanonicalPartitions();
      throw new AssertionError("canonical export used native canonical partitions");
    }

    @Override
    protected String[] checkpointRocksDBHandle(String path) {
      if (checkpoint || path.isEmpty()) return super.checkpointRocksDBHandle(path);
      throw new AssertionError("canonical export created native checkpoint files");
    }
  }

  @Override
  public void close() throws Exception {
    harness.close();
  }
}
