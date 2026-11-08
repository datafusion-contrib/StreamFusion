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
  private final RowType inputType, outputType, keyType;
  final OneInputStreamOperatorTestHarness<?, ?> harness;
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock;
  final KeyedOneInputStreamOperatorTestHarness<RowData, ArrowBatch, ArrowBatch> nativeOwner;
  final Object function;
  private boolean cancelled;
  private final OneInputStreamOperatorTestHarness<RowData, RowData> stockLocal;
  private final OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> nativeLocal;
  private final RowDataSerializer partialSerializer;

  @SuppressWarnings("unchecked")
  ManagedUdfGroupStockFixture(boolean nativeRun, boolean mini, boolean rocks, int cache, long ttl)
      throws Exception {
    this(nativeRun, mini, rocks, cache, ttl, false, false);
  }

  @SuppressWarnings("unchecked")
  ManagedUdfGroupStockFixture(
      boolean nativeRun,
      boolean mini,
      boolean rocks,
      int cache,
      long ttl,
      boolean global,
      boolean faulting)
      throws Exception {
    this(nativeRun, mini, rocks, cache, ttl, global, faulting, RowType.of(new BigIntType()));
  }

  @SuppressWarnings("unchecked")
  ManagedUdfGroupStockFixture(
      boolean nativeRun,
      boolean mini,
      boolean rocks,
      int cache,
      long ttl,
      boolean global,
      boolean faulting,
      RowType keyType)
      throws Exception {
    this.keyType = keyType;
    long planningStarted = tech.streamfusion.CiFixtureTimings.start();
    var inputFields = new ArrayList<LogicalType>(keyType.getChildren());
    inputFields.add(new BigIntType());
    inputType = RowType.of(inputFields.toArray(new LogicalType[0]));
    inputFields.add(new BigIntType());
    outputType = RowType.of(inputFields.toArray(new LogicalType[0]));
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "false");
    config.setString("pipeline.max-parallelism", "128");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "5");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.optimizer.agg-phase-strategy", global ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.state.ttl", ttl + " ms");
    table.createTemporarySystemFunction(
        "fixture_history",
        faulting
            ? org.example.canonical.FaultingMergeHistoryAggregate.class
            : global
                ? CanonicalFixtureFactory.MergeHistoryAggregate.class
                : CanonicalFixtureFactory.HistoryAggregate.class);
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
        "CREATE TABLE input_table("
            + keyDdl(keyType)
            + ",v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='false')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table("
            + keyDdl(keyType)
            + ",h BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var graph =
        CanonicalRegularJoinPlanTest.graph(
            planner,
            table,
            CanonicalRegularJoinPlanTest.translate(
                planner,
                "INSERT INTO output_table SELECT "
                    + keyColumns(keyType)
                    + ",fixture_history(v),COUNT(*) FROM fixture.db.input_table GROUP BY "
                    + keyColumns(keyType)));
    tech.streamfusion.CiFixtureTimings.record("managed-udf.plan", planningStarted);
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
    JvmGroupPartialTransport transport = null;
    if (global) {
      var localNode =
          graph.getStreamNodes().stream()
              .filter(
                  n ->
                      n.getOperatorFactory() instanceof SimpleOperatorFactory
                          && ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                  .getOperator()
                                  .getClass()
                              == org.apache.flink.table.runtime.operators.bundle.MapBundleOperator
                                  .class)
              .findFirst()
              .orElseThrow();
      var localOperator =
          (OneInputStreamOperator<RowData, RowData>)
              ((SimpleOperatorFactory<?>) localNode.getOperatorFactory()).getOperator();
      var localSelector = (RowDataKeySelector) field(localOperator, "keySelector");
      partialSerializer = (RowDataSerializer) localNode.getTypeSerializerOut();
      var partialType = RowType.of((LogicalType[]) field(partialSerializer, "types"));
      transport =
          new JvmGroupPartialTransport(partialSerializer, partialType, keyType.getFieldCount());
      if (nativeRun) {
        nativeLocal =
            new OneInputStreamOperatorTestHarness<>(
                new NativeColumnarLocalJvmGroupOperator(
                    inputType,
                    transport.type(),
                    (MiniBatchLocalGroupAggFunction) field(localOperator, "function"),
                    (BundleTrigger<RowData>) field(localOperator, "bundleTrigger"),
                    localSelector,
                    3,
                    transport));
        nativeLocal.setup(new ArrowBatchSerializer());
        stockLocal = null;
      } else {
        stockLocal = new OneInputStreamOperatorTestHarness<>(localOperator);
        stockLocal.setup(partialSerializer);
        nativeLocal = null;
      }
    } else {
      stockLocal = null;
      nativeLocal = null;
      partialSerializer = null;
    }
    var physicalInput = global ? transport.type() : inputType;
    if (nativeRun) {
      var owner =
          new NativeColumnarJvmGroupOperator(
              physicalInput,
              outputType,
              mini ? null : (GroupAggFunction) function,
              mini
                  ? (org.apache.flink.table.runtime.operators.bundle.MapBundleFunction<
                          RowData, ?, RowData, RowData>)
                      function
                  : null,
              mini ? (BundleTrigger<RowData>) field(original, "bundleTrigger") : null,
              selector,
              3,
              cache,
              keyType.getFieldCount() == 1
                      && java.util.Set.of(
                              LogicalTypeRoot.INTEGER,
                              LogicalTypeRoot.BIGINT,
                              LogicalTypeRoot.VARCHAR)
                          .contains(keyType.getTypeAt(0).getTypeRoot())
                  ? keyType.getTypeAt(0).getTypeRoot()
                  : null,
              0,
              ttl,
              transport,
              null);
      nativeOwner =
          new KeyedOneInputStreamOperatorTestHarness<>(
              owner,
              new ArrowBatchRowKeySelector(physicalInput, selector),
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
      stock.setup(new RowDataSerializer(outputType));
      nativeOwner = null;
      harness = stock;
    }
  }

  static String keyColumns(RowType keys) {
    return java.util.stream.IntStream.range(0, keys.getFieldCount())
        .mapToObj(i -> "k" + i)
        .collect(java.util.stream.Collectors.joining(","));
  }

  static String keyDdl(RowType keys) {
    return java.util.stream.IntStream.range(0, keys.getFieldCount())
        .mapToObj(i -> "k" + i + " " + keys.getTypeAt(i).asSerializableString())
        .collect(java.util.stream.Collectors.joining(","));
  }

  void open(OperatorSubtaskState saved, long time) throws Exception {
    clock(time);
    if (saved != null) harness.initializeState(saved);
    if (stockLocal != null) stockLocal.open();
    if (nativeLocal != null) nativeLocal.open();
    harness.open();
  }

  void clock(long time) throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
    if (stockLocal != null) {
      stockLocal.setProcessingTime(time);
      stockLocal.setStateTtlProcessingTime(time);
    }
    if (nativeLocal != null) {
      nativeLocal.setProcessingTime(time);
      nativeLocal.setStateTtlProcessingTime(time);
    }
  }

  Map<String, List<String>> feed(List<RowData> rows, boolean split) throws Exception {
    harness.getOutput().clear();
    if (stockLocal != null) {
      var copy = new RowDataSerializer(inputType);
      for (var row : rows) stockLocal.processElement(new StreamRecord<>(copy.copy(row)));
      flushLocal();
    } else if (nativeLocal != null) {
      var batches =
          split
              ? rows.stream().map(List::of).collect(java.util.stream.Collectors.toList())
              : List.of(rows);
      for (var batch : batches)
        nativeLocal.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(batch, inputType, NativeAllocator.SHARED, true))));
      flushLocal();
    } else if (stock != null) {
      var copy = new RowDataSerializer(inputType);
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
                    RowDataArrowConverter.write(batch, inputType, NativeAllocator.SHARED, true))));
    }
    harness.prepareSnapshotPreBarrier(1);
    var result = new TreeMap<String, List<String>>();
    var serializer = new RowDataSerializer(outputType);
    Object element;
    while ((element = harness.getOutput().poll()) != null) {
      if (!(element instanceof StreamRecord)) continue;
      Object value = ((StreamRecord<?>) element).getValue();
      if (value instanceof ArrowBatch) {
        try (var root = ((ArrowBatch) value).root()) {
          for (var row : RowDataArrowConverter.read(root, outputType))
            record(result, row, serializer);
        }
      } else record(result, (RowData) value, serializer);
    }
    return result;
  }

  private void record(Map<String, List<String>> result, RowData row, RowDataSerializer serializer)
      throws Exception {
    String key;
    if (keyType.getFieldCount() == 1 && keyType.getTypeAt(0).is(LogicalTypeRoot.BIGINT))
      key = row.isNullAt(0) ? "null" : Long.toString(row.getLong(0));
    else {
      var projected = new org.apache.flink.table.data.GenericRowData(keyType.getFieldCount());
      for (int i = 0; i < keyType.getFieldCount(); i++)
        projected.setField(
            i, RowData.createFieldGetter(keyType.getTypeAt(i), i).getFieldOrNull(row));
      var keyBytes = new DataOutputSerializer(64);
      new RowDataSerializer(keyType).serialize(projected, keyBytes);
      key = Base64.getEncoder().encodeToString(keyBytes.getCopyOfBuffer());
    }
    var bytes = new DataOutputSerializer(64);
    serializer.serialize(row, bytes);
    result
        .computeIfAbsent(key, ignored -> new ArrayList<>())
        .add(Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
  }

  private void flushLocal() throws Exception {
    if (stockLocal != null) {
      stockLocal.prepareSnapshotPreBarrier(1);
      Object element;
      while ((element = stockLocal.getOutput().poll()) != null)
        if (element instanceof StreamRecord)
          stock.processElement(
              new StreamRecord<>(
                  partialSerializer.copy((RowData) ((StreamRecord<?>) element).getValue())));
      assertTrue(((Map<?, ?>) field(stockLocal.getOperator(), "bundle")).isEmpty());
    }
    if (nativeLocal != null) {
      nativeLocal.prepareSnapshotPreBarrier(1);
      Object element;
      while ((element = nativeLocal.getOutput().poll()) != null)
        if (element instanceof StreamRecord)
          nativeOwner.processElement(
              new StreamRecord<>((ArrowBatch) ((StreamRecord<?>) element).getValue()));
      assertTrue(
          ((Map<?, ?>) field(field(nativeLocal.getOperator(), "miniBatch"), "buffer")).isEmpty());
    }
  }

  OperatorSubtaskState checkpoint() throws Exception {
    flushLocal();
    harness.prepareSnapshotPreBarrier(1);
    return harness.snapshot(1, 0);
  }

  OperatorSubtaskState canonical() throws Exception {
    flushLocal();
    return CanonicalGroupArithmeticInitializationTest.snapshot(harness);
  }

  Map<String, String> rawState() throws Exception {
    flushLocal();
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
    if (!cancelled) {
      try {
        if (stockLocal != null) stockLocal.close();
        if (nativeLocal != null) nativeLocal.close();
      } finally {
        harness.close();
      }
    }
  }

  void cancel() throws Exception {
    // The released harness's normal close calls finish, which a failed task must not execute.
    cancelled = true;
    try {
      if (stockLocal != null) cancelHarness(stockLocal);
      if (nativeLocal != null) cancelHarness(nativeLocal);
    } finally {
      cancelHarness(harness);
    }
  }

  private static void cancelHarness(OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
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
