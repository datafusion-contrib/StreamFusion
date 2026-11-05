package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.*;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.*;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.example.canonical.CanonicalFixtureFactory;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.*;

final class ManagedUdfGroupStockFixture implements AutoCloseable {
  static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());
  static final RowType OUTPUT = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  final OneInputStreamOperatorTestHarness<?, ?> harness;
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock;
  final KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch> nativeOwner;
  final Object function;
  private boolean cancelled;

  @SuppressWarnings("unchecked")
  ManagedUdfGroupStockFixture(boolean nativeRun, boolean mini, boolean rocks, int cache, long ttl)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "false");
    config.setString("pipeline.max-parallelism", "128");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "5");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.state.ttl", ttl + " ms");
    table.createTemporarySystemFunction(
        "fixture_history", CanonicalFixtureFactory.HistoryAggregate.class);
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return Optional.of(new CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='false')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,h BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var graph =
        CanonicalRegularJoinPlanTest.graph(
            planner,
            table,
            CanonicalRegularJoinPlanTest.translate(
                planner,
                "INSERT INTO output_table SELECT k,fixture_history(v),COUNT(*) FROM"
                    + " fixture.db.input_table GROUP BY k"));
    var node =
        graph.getStreamNodes().stream()
            .filter(
                n -> {
                  if (!(n.getOperatorFactory() instanceof SimpleOperatorFactory)) return false;
                  var op = ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator();
                  return mini
                      ? op.getClass() == KeyedMapBundleOperator.class
                      : op instanceof KeyedProcessOperator
                          && ((KeyedProcessOperator<?, ?, ?>) op).getUserFunction()
                              instanceof GroupAggFunction;
                })
            .findFirst()
            .orElseThrow();
    var original =
        (OneInputStreamOperator<RowData, RowData>)
            ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    function =
        mini
            ? field(original, "function")
            : ((KeyedProcessOperator<?, ?, ?>) original).getUserFunction();
    var selector = (RowDataKeySelector) node.getStatePartitioners()[0];
    if (nativeRun) {
      var owner =
          new NativeColumnarJvmGroupOperator(
              INPUT,
              OUTPUT,
              mini ? null : (GroupAggFunction) function,
              mini ? (MiniBatchGroupAggFunction) function : null,
              mini ? (BundleTrigger<RowData>) field(original, "bundleTrigger") : null,
              selector,
              3,
              cache,
              LogicalTypeRoot.BIGINT,
              0,
              ttl);
      nativeOwner =
          new KeyedOneInputStreamOperatorTestHarness<>(
              owner,
              new ArrowBatchRowKeySelector(INPUT, selector),
              selector.getProducedType(),
              128,
              1,
              0);
      nativeOwner.setStateBackend(
          rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
      nativeOwner.setup(new ArrowBatchSerializer());
      stock = null;
      harness = nativeOwner;
    } else {
      stock =
          new KeyedOneInputStreamOperatorTestHarness<>(
              original, selector, selector.getProducedType(), 128, 1, 0);
      stock.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
      stock.setup(new RowDataSerializer(OUTPUT));
      nativeOwner = null;
      harness = stock;
    }
  }

  void open(OperatorSubtaskState saved, long time) throws Exception {
    clock(time);
    if (saved != null) harness.initializeState(saved);
    harness.open();
  }

  void clock(long time) throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
  }

  Map<String, List<String>> feed(List<RowData> rows, boolean split) throws Exception {
    harness.getOutput().clear();
    if (stock != null) {
      var copy = new RowDataSerializer(INPUT);
      for (var row : rows) stock.processElement(new StreamRecord<>(copy.copy(row)));
    } else {
      var batches =
          split
              ? rows.stream().map(List::of).collect(java.util.stream.Collectors.toList())
              : List.of(rows);
      for (var batch : batches)
        nativeOwner.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(batch, INPUT, NativeAllocator.SHARED, true))));
    }
    harness.prepareSnapshotPreBarrier(1);
    var result = new TreeMap<String, List<String>>();
    var serializer = new RowDataSerializer(OUTPUT);
    Object element;
    while ((element = harness.getOutput().poll()) != null) {
      if (!(element instanceof StreamRecord)) continue;
      Object value = ((StreamRecord<?>) element).getValue();
      if (value instanceof ArrowBatch) {
        try (var root = ((ArrowBatch) value).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) record(result, row, serializer);
        }
      } else record(result, (RowData) value, serializer);
    }
    return result;
  }

  private static void record(
      Map<String, List<String>> result, RowData row, RowDataSerializer serializer)
      throws Exception {
    String key = row.isNullAt(0) ? "null" : Long.toString(row.getLong(0));
    var bytes = new DataOutputSerializer(64);
    serializer.serialize(row, bytes);
    result
        .computeIfAbsent(key, ignored -> new ArrayList<>())
        .add(Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
  }

  OperatorSubtaskState canonical() throws Exception {
    return CanonicalGroupArithmeticInitializationTest.snapshot(harness);
  }

  Map<String, String> rawState() throws Exception {
    harness.prepareSnapshotPreBarrier(1);
    var backend =
        (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
            harness.getOperator().<RowData>getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      var result = new TreeMap<String, String>();
      var metadata = resources.getMetaInfoSnapshots();
      try (var iterator = resources.createKVStateIterator()) {
        while (iterator.isValid()) {
          String name = metadata.get(iterator.kvStateId()).getName();
          assertFalse(name.startsWith("__streamfusion"), name);
          result.put(
              name + ":" + Base64.getEncoder().encodeToString(iterator.key()),
              Base64.getEncoder().encodeToString(iterator.value()));
          iterator.next();
        }
      }
      return result;
    }
  }

  @Override
  public void close() throws Exception {
    if (!cancelled) harness.close();
  }

  void cancel() throws Exception {
    // The released harness's normal close calls finish, which a failed task must not execute.
    cancelled = true;
    harness.getProcessingTimeService().shutdownService();
    try {
      harness.getOperator().close();
    } finally {
      try {
        harness.getEnvironment().close();
      } finally {
        var cleanup = MockStreamTask.class.getDeclaredMethod("cleanUpInternal");
        cleanup.setAccessible(true);
        cleanup.invoke(field(harness, "mockTask"));
      }
    }
  }
}
