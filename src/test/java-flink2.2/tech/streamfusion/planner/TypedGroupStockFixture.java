package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
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
import tech.streamfusion.state.*;

final class TypedGroupStockFixture implements AutoCloseable {
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
  final RowDataKeySelector selector;
  final RowType inputType, outputType, accumulatorType;
  final Object function;
  final StockGroupTypedStateContract contract;
  final NativeColumnarGroupAggregateOperator template;
  final boolean rocks;

  @SuppressWarnings("unchecked")
  TypedGroupStockFixture(String numeric, boolean append, boolean rocks) throws Exception {
    this.rocks = rocks;
    var graph = graph(numeric, append, false);
    var node =
        graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                            instanceof KeyedProcessOperator
                        && ((KeyedProcessOperator<?, ?, ?>)
                                    ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                        .getOperator())
                                .getUserFunction()
                            instanceof GroupAggFunction)
            .findFirst()
            .orElseThrow();
    var operator =
        (KeyedProcessOperator<RowData, RowData, RowData>)
            ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    function = operator.getUserFunction();
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
    assertTrue(inputType.getTypeAt(3).is(LogicalTypeRoot.VARCHAR));
    assertTrue(inputType.getTypeAt(4).is(LogicalTypeRoot.BOOLEAN));
    harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
    harness.setup(
        (org.apache.flink.api.common.typeutils.TypeSerializer<RowData>)
            node.getTypeSerializerOut());
    var nativeGraph = graph(numeric, append, true);
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
    assertEquals(1, views.size());
    assertEquals(2, ((int[]) field(views.get(0), "aggregates")).length);
  }

  private static StreamGraph graph(String numeric, boolean append, boolean nativeEnabled)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    config.setString("pipeline.max-parallelism", "128");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
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
    table.executeSql(
        "CREATE TABLE output_table(k STRING,t TIMESTAMP(3),n BIGINT,total "
            + numeric
            + ",a "
            + average
            + ",d BIGINT,e BIGINT"
            + (append ? ",lo " + numeric + ",hi " + numeric : "")
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,t,COUNT(*),SUM(v),AVG(v),COUNT(DISTINCT"
            + " s),COUNT(DISTINCT s) FILTER(WHERE f)"
            + (append ? ",MIN(v),MAX(v)" : "")
            + " FROM fixture.db.input_table GROUP BY k,t";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    return CanonicalRegularJoinPlanTest.graph(
        planner, table, CanonicalRegularJoinPlanTest.translate(planner, query));
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
          false,
          -1,
          0,
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
      if (checkpoint) return super.checkpointRocksDBHandle(path);
      throw new AssertionError("canonical export checkpointed native RocksDB");
    }
  }

  @Override
  public void close() throws Exception {
    harness.close();
  }
}
