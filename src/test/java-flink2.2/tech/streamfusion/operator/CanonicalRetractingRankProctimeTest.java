package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.state.StockRetractingTopNStateContract;

class CanonicalRetractingRankProctimeTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,true,false",
    "true,false,false",
    "true,true,false",
    "false,false,true",
    "false,true,true",
    "true,false,true",
    "true,true,true"
  })
  void actualSqlRetainedProctimePlaceholderImportsFromCanonicalState(
      boolean stockRocks, boolean nativeRocks, boolean nativeCheckpoint) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "false");
    config.setString("table.exec.state.ttl", "0 ms");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT,payload BIGINT NOT NULL,pt AS PROCTIME())"
            + " WITH ('connector'='canonical-fixture','input'='/unused')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,payload BIGINT,pt TIMESTAMP_LTZ(3),rn BIGINT)"
            + " WITH ('connector'='blackhole')");
    var json =
        table
            .compilePlanSql(
                "INSERT INTO output_table SELECT k,v,payload,pt,rn FROM (SELECT *,ROW_NUMBER() OVER"
                    + " (PARTITION BY k ORDER BY v ASC) AS rn FROM input_table) WHERE rn<=3")
            .asJsonString();
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var node = rank(planner, json);
    var input = (RowType) node.getInputEdges().get(0).getOutputType();
    var output = (RowType) node.getOutputType();
    int pt =
        java.util.stream.IntStream.range(0, input.getFieldCount())
            .filter(index -> LogicalTypeChecks.isProctimeAttribute(input.getTypeAt(index)))
            .findFirst()
            .orElse(-1);
    assertTrue(pt >= 0, input.toString());
    assertTrue(LogicalTypeChecks.isProctimeAttribute(input.getTypeAt(pt)), input.toString());
    assertFalse(input.getTypeAt(pt).isNullable());
    var original = transform(node, planner);
    var selector = (RowDataKeySelector) original.getStateKeySelector();
    var function =
        ((KeyedProcessOperator<?, ?, ?>)
                ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator())
            .getUserFunction();
    assertEquals(RetractableTopNFunction.class, function.getClass());
    var sorts = (RowDataKeySelector) field(function, "sortKeySelector");
    var contract =
        new StockRetractingTopNStateContract(
            selector.getProducedType().toSerializer(),
            selector,
            sorts,
            selector.getProducedType().toRowType(),
            input,
            (ComparableRecordComparator) field(function, "serializableComparator"),
            false,
            false);
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState saved;
    try (var stock = harness(original, selector, output, stockRocks)) {
      stock.open();
      for (long value : new long[] {20, 10, 30}) {
        var row = new GenericRowData(input.getFieldCount());
        row.setField(input.getFieldNames().indexOf("k"), 1L);
        row.setField(input.getFieldNames().indexOf("v"), value);
        row.setField(input.getFieldNames().indexOf("payload"), 99L);
        assertTrue(row.isNullAt(pt));
        stock.processElement(
            new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
      }
      stock.prepareSnapshotPreBarrier(1);
      saved =
          stock
              .snapshotWithLocalState(
                  1,
                  0,
                  org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                      org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
    }
    try (var restored =
        harness(transform(rank(planner, json), planner), selector, output, stockRocks)) {
      restored.initializeState(saved);
      restored.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              restored.getOperator().<RowData>getKeyedStateBackend();
      RowData partition;
      try (var keys = backend.getKeys("data-state", VoidNamespace.INSTANCE)) {
        partition = keys.findFirst().orElseThrow();
      }
      backend.setCurrentKey(partition);
      MapState<RowData, List<RowData>> rows =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "data-state",
                  new RowDataSerializer(sorts.getProducedType().toRowType()),
                  new ListSerializer<>(new RowDataSerializer(input))));
      int retained = 0;
      for (var entry : rows.entries())
        for (var row : entry.getValue()) {
          assertTrue(
              row.isNullAt(pt), "Actual released state must retain the physical placeholder");
          retained++;
        }
      assertEquals(3, retained);
      var expectedState = CanonicalUnboundedOverScalarTest.raw(restored);
      long allocated = NativeAllocator.SHARED.getAllocatedMemory();
      boolean before = (Boolean) field(function, "generateUpdateBefore");
      long target = create(input, nativeRocks, before, new byte[0][]);
      try {
        contract.configureNativeState(target, nativeRocks);
        var entry = rows.entries().iterator().next();
        var sortKey =
            new RowDataSerializer(sorts.getProducedType().toRowType()).copy(entry.getKey());
        var valid = new java.util.ArrayList<RowData>();
        var serializer = new RowDataSerializer(input);
        for (var item : entry.getValue()) valid.add(serializer.copy(item));
        var invalid = new java.util.ArrayList<>(valid);
        var damaged = new GenericRowData(input.getFieldCount());
        var originalRow = valid.get(0);
        damaged.setRowKind(originalRow.getRowKind());
        for (int column = 0; column < input.getFieldCount(); column++)
          damaged.setField(
              column,
              RowData.createFieldGetter(input.getTypeAt(column), column)
                  .getFieldOrNull(originalRow));
        damaged.setField(input.getFieldNames().indexOf("payload"), null);
        invalid.set(0, serializer.toBinaryRow(damaged).copy());
        rows.put(sortKey, invalid);
        var corruptState = CanonicalUnboundedOverScalarTest.raw(restored);
        long denied = target;
        var failure =
            assertThrows(
                IllegalStateException.class,
                () -> contract.importState(backend, denied, nativeRocks));
        assertEquals("NULL in a non-null retracting row", failure.getMessage());
        assertEquals(corruptState, CanonicalUnboundedOverScalarTest.raw(restored));
        assertEquals(
            0, snapshot(target, nativeRocks).length, "Preflight must leave the target empty");
        backend.setCurrentKey(partition);
        rows.put(sortKey, valid);
        assertEquals(expectedState, CanonicalUnboundedOverScalarTest.raw(restored));
        contract.importState(backend, target, nativeRocks);
        if (nativeCheckpoint) {
          var checkpoint = snapshot(target, nativeRocks);
          assertTrue(checkpoint.length > 0);
          for (int index = 0; index < checkpoint.length; index++)
            checkpoint[index] =
                java.util.Arrays.copyOfRange(checkpoint[index], 4, checkpoint[index].length);
          close(target, nativeRocks);
          target = 0;
          target = create(input, nativeRocks, before, checkpoint);
        }
        contract.exportState(backend, target, nativeRocks);
        assertEquals(expectedState, CanonicalUnboundedOverScalarTest.raw(restored));
        var deleted = row(input, 20);
        deleted.setRowKind(org.apache.flink.types.RowKind.DELETE);
        var suffix = List.of(deleted, row(input, 15));
        CanonicalAppendRankTypedTest.feed(restored, input, suffix);
        var expected = CanonicalAppendRankTypedTest.bytes(output, restored.extractOutputValues());
        assertEquals(
            expected,
            CanonicalAppendRankTypedTest.bytes(
                output, push(target, nativeRocks, input, output, suffix)));
        var after = CanonicalUnboundedOverScalarTest.raw(restored);
        contract.exportState(backend, target, nativeRocks);
        assertEquals(after, CanonicalUnboundedOverScalarTest.raw(restored));
        restored.prepareSnapshotPreBarrier(2);
        var returnedState =
            restored
                .snapshotWithLocalState(
                    2,
                    0,
                    org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                        org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState();
        try (var returned =
            harness(transform(rank(planner, json), planner), selector, output, stockRocks)) {
          returned.initializeState(returnedState);
          returned.open();
          assertEquals(after, CanonicalUnboundedOverScalarTest.raw(returned));
          var future = List.of(row(input, 5));
          CanonicalAppendRankTypedTest.feed(returned, input, future);
          assertEquals(
              CanonicalAppendRankTypedTest.bytes(output, returned.extractOutputValues()),
              CanonicalAppendRankTypedTest.bytes(
                  output, push(target, nativeRocks, input, output, future)));
        }
      } finally {
        if (target != 0) close(target, nativeRocks);
      }
      assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    }
  }

  private static RowData row(RowType input, long value) {
    var row = new GenericRowData(input.getFieldCount());
    row.setField(input.getFieldNames().indexOf("k"), 1L);
    row.setField(input.getFieldNames().indexOf("v"), value);
    row.setField(input.getFieldNames().indexOf("payload"), 99L);
    return row;
  }

  private long create(RowType input, boolean rocks, boolean before, byte[][] snapshots)
      throws Exception {
    int[] keys = {0}, precision = {Native.STOCK_KEY_PROJECTION, -1}, sort = {1}, order = {1};
    if (!rocks)
      return snapshots.length == 0
          ? Native.createTopNRanker(
              keys, precision, sort, order, order, 0, 3, -1, true, true, before, false, 0, 1 << 24)
          : Native.restoreTopNRankerPartitions(
              keys, precision, sort, order, order, 0, 3, -1, true, true, before, false, 0, 1000,
              snapshots, 1 << 24);
    try (var schema = org.apache.arrow.c.ArrowSchema.allocateNew(NativeAllocator.SHARED);
        AutoCloseable release =
            () -> {
              if (schema.snapshot().release != 0) schema.release();
            }) {
      org.apache.arrow.c.Data.exportSchema(
          NativeAllocator.SHARED,
          tech.streamfusion.arrow.ArrowConversion.toArrowSchema(input),
          NativeAllocator.DICTIONARIES,
          schema);
      return Native.createRocksDBTopNRanker(
          keys,
          precision,
          sort,
          order,
          order,
          0,
          3,
          -1,
          true,
          true,
          before,
          false,
          0,
          0,
          1 << 24,
          schema.memoryAddress(),
          directory.resolve("native-" + java.util.UUID.randomUUID()).toString(),
          128,
          tech.streamfusion.state.FlinkRocksDBOptions.from(new Configuration()).json(),
          0,
          new String[0],
          new String[0],
          0,
          127,
          false,
          snapshots);
    }
  }

  private static byte[][] snapshot(long handle, boolean rocks) {
    return rocks
        ? Native.snapshotRocksDBTopNRankerPartitions(handle)
        : Native.snapshotTopNRankerPartitions(
            handle, 128, new int[] {Native.STOCK_KEY_PROJECTION, -1});
  }

  private static void close(long handle, boolean rocks) {
    if (rocks) Native.closeRocksDBTopNRanker(handle);
    else Native.closeTopNRanker(handle);
  }

  private static List<RowData> push(
      long handle, boolean rocks, RowType input, RowType output, List<RowData> rows) {
    try (var root = RowDataArrowConverter.write(rows, input, NativeAllocator.SHARED, true);
        var array = org.apache.arrow.c.ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = org.apache.arrow.c.ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var out = org.apache.arrow.c.ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outSchema = org.apache.arrow.c.ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      org.apache.arrow.c.Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (rocks)
        Native.pushRocksDBTopNRanker(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            1000,
            out.memoryAddress(),
            outSchema.memoryAddress());
      else
        Native.pushTopNRanker(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            1000,
            out.memoryAddress(),
            outSchema.memoryAddress());
      try (var result =
          org.apache.arrow.c.Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, out, outSchema, NativeAllocator.DICTIONARIES)) {
        var reader = tech.streamfusion.arrow.ArrowConversion.createArrowReader(result, output);
        var serializer = new RowDataSerializer(output);
        var values = new java.util.ArrayList<RowData>();
        var kinds =
            (org.apache.arrow.vector.TinyIntVector) result.getVector(output.getFieldCount());
        for (int i = 0; i < result.getRowCount(); i++) {
          var row = serializer.copy(reader.read(i));
          row.setRowKind(org.apache.flink.types.RowKind.fromByteValue(kinds.get(i)));
          values.add(row);
        }
        return values;
      }
    }
  }

  private static Object field(Object object, String name) throws Exception {
    for (Class<?> type = object.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
      } catch (NoSuchFieldException ignored) {
      }
    }
    throw new NoSuchFieldException(name);
  }

  private static ExecNode<?> rank(PlannerBase planner, String json) throws Exception {
    var graph =
        CompiledPlanSerdeUtil.createJsonObjectReader(planner.createSerdeContext())
            .forType(ExecNodeGraph.class)
            .<ExecNodeGraph>readValue(json);
    return find(graph.getRootNodes().get(0));
  }

  private static ExecNode<?> find(ExecNode<?> node) {
    if (node instanceof StreamExecRank) return node;
    for (var edge : node.getInputEdges()) {
      var result = find(edge.getSource());
      if (result != null) return result;
    }
    return null;
  }

  private static OneInputTransformation<?, ?> transform(ExecNode<?> node, PlannerBase planner) {
    return (OneInputTransformation<?, ?>) node.translateToPlan(planner);
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      OneInputTransformation<?, ?> transform,
      RowDataKeySelector selector,
      RowType output,
      boolean rocks)
      throws Exception {
    var operator =
        (KeyedProcessOperator<RowData, RowData, RowData>)
            ((SimpleOperatorFactory<?>) transform.getOperatorFactory()).getOperator();
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend()
            : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
    harness.setup(new RowDataSerializer(output));
    return harness;
  }
}
