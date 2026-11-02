package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.backend;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockDedupStateContract;

class CanonicalEagerDedupInteropTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return Stream.of("proc-first", "proc-last", "row-first", "row-last")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .flatMap(
                                    ttl ->
                                        Stream.of("nullable", "not-null", "keyless")
                                            .map(shape -> Arguments.of(mode, rocks, ttl, shape)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void selectedRowsMarkersAndKindsContinueAcrossBothEngineSwitches(
      String mode, boolean rocks, boolean ttl, String shape) throws Exception {
    RowType type =
        RowType.of(
            new BigIntType(!shape.equals("not-null")), new BigIntType(), new TimestampType(3));
    int[] keys = shape.equals("keyless") ? new int[0] : new int[] {0};
    RowType keyType = keys.length == 0 ? RowType.of() : RowType.of(type.getTypeAt(0));
    boolean rowtime = mode.startsWith("row");
    boolean first = mode.endsWith("first");
    long retention = ttl ? 1000 : 0;
    long handle = create(rocks, keys, rowtime, first, retention);
    var prefix =
        List.<RowData>of(
            row(1L, 10, 1000),
            row(1L, 10, 1000),
            row(2L, 20, 2000),
            row(1L, 15, 1500),
            row(shape.equals("not-null") ? 3L : null, 30, 3000));
    var suffix =
        List.<RowData>of(row(1L, 15, 1500), row(1L, 9, 900), row(2L, 25, 2500), row(4L, 40, 4000));
    var future = List.<RowData>of(row(1L, 8, 800), row(4L, 45, 4500));
    try (var stock = harness(type, keys, mode, retention, true);
        var view = harness(type, keys, mode, 0, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1500);
      stockPush(stock, type, prefix);
      view.initializeState(snapshot(stock));
      view.open();
      view.setStateTtlProcessingTime(10000);
      var contract =
          new StockDedupStateContract(
              new RowDataSerializer(keyType),
              keyType,
              type,
              first && !rowtime,
              rowtime ? 2 : -1,
              retention);
      contract.importState(backend(view), handle, rocks);
      stock.setStateTtlProcessingTime(1800);
      assertEquals(
          stockPush(stock, type, suffix),
          nativePush(handle, rocks, type, suffix, 1800, first && !rowtime));
      contract.exportState(backend(view), handle, rocks);
      try (var restored = harness(type, keys, mode, retention, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        restored.setStateTtlProcessingTime(1900);
        stock.setStateTtlProcessingTime(1900);
        var expected = stockPush(stock, type, future);
        assertEquals(expected, stockPush(restored, type, future));
        assertEquals(expected, nativePush(handle, rocks, type, future, 1900, first && !rowtime));
      }
    } finally {
      if (rocks) Native.closeRocksDBKeepLastDeduplicator(handle);
      else Native.closeKeepLastDeduplicator(handle);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void physicalNullProctimePlaceholderSurvivesBothConversions(boolean rocks) throws Exception {
    var type =
        RowType.of(
            new BigIntType(false),
            new BigIntType(),
            new org.apache.flink.table.types.logical.LocalZonedTimestampType(
                false, org.apache.flink.table.types.logical.TimestampKind.PROCTIME, 3));
    var physical = RowType.of(type.getTypeAt(0), type.getTypeAt(1), type.getTypeAt(2).copy(true));
    int[] keys = {0};
    var keyType = RowType.of(type.getTypeAt(0));
    long handle = create(rocks, keys, false, false, 0);
    var duplicate = List.<RowData>of(GenericRowData.of(1L, 10L, null));
    try (var stock = harness(type, keys, "proc-last", 0, true);
        var view = harness(type, keys, "proc-last", 0, false)) {
      stock.open();
      stockPush(stock, type, duplicate);
      view.initializeState(snapshot(stock));
      view.open();
      var contract =
          new StockDedupStateContract(new RowDataSerializer(keyType), keyType, type, false, -1, 0);
      var backend = backend(view);
      contract.importState(backend, handle, rocks);
      assertEquals(List.of(), stockPush(stock, type, duplicate));
      assertEquals(List.of(), nativePush(handle, rocks, physical, duplicate, 0, false));
      contract.exportState(backend, handle, rocks);
      var state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("deduplicate-state", new RowDataSerializer(type)));
      backend.setCurrentKey(
          new RowDataSerializer(keyType).toBinaryRow(GenericRowData.of(1L)).copy());
      assertTrue(state.value().isNullAt(2));
      try (var restored = harness(type, keys, "proc-last", 0, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        assertEquals(List.of(), stockPush(restored, type, duplicate));
      }
    } finally {
      close(handle, rocks);
    }
  }

  static Stream<Arguments> ttlCases() {
    return Stream.of("proc-first", "proc-last", "row-first", "row-last")
        .flatMap(mode -> Stream.of(false, true).map(rocks -> Arguments.of(mode, rocks)));
  }

  @ParameterizedTest
  @MethodSource("ttlCases")
  void rawWriteAgeSurvivesConversionAndExpiresAtTheOriginalBoundary(String mode, boolean rocks)
      throws Exception {
    var type = RowType.of(new BigIntType(false), new BigIntType(), new TimestampType(3));
    int[] keys = {0};
    var keyType = RowType.of(type.getTypeAt(0));
    boolean rowtime = mode.startsWith("row");
    boolean first = mode.endsWith("first");
    long handle = create(rocks, keys, rowtime, first, 1000);
    try (var stock = harness(type, keys, mode, 1000, true);
        var view = harness(type, keys, mode, 0, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1500);
      stockPush(stock, type, List.of(row(1L, 10, 1000)));
      view.initializeState(snapshot(stock));
      view.open();
      view.setStateTtlProcessingTime(10000);
      var contract =
          new StockDedupStateContract(
              new RowDataSerializer(keyType),
              keyType,
              type,
              first && !rowtime,
              rowtime ? 2 : -1,
              1000);
      var backend = backend(view);
      contract.importState(backend, handle, rocks);
      contract.exportState(backend, handle, rocks);
      var raw =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "deduplicate-state",
                  new TtlSerializer<>(
                      LongSerializer.INSTANCE, valueSerializer(type, first && !rowtime))));
      backend.setCurrentKey(
          new RowDataSerializer(keyType).toBinaryRow(GenericRowData.of(1L)).copy());
      assertEquals(1500, raw.value().getLastAccessTimestamp());
      try (var restored = harness(type, keys, mode, 1000, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        restored.setStateTtlProcessingTime(2500);
        var next = List.<RowData>of(row(1L, 20, 2000));
        assertEquals(List.of("+I|1|20|2000"), stockPush(restored, type, next));
        assertEquals(
            List.of("+I|1|20|2000"),
            nativePush(handle, rocks, type, next, 2500, first && !rowtime));
      }
    } finally {
      close(handle, rocks);
    }
  }

  static Stream<Arguments> rejectionCases() {
    return Stream.of("marker-key", "row-key", "row-kind", "row-payload")
        .flatMap(mode -> Stream.of(false, true).map(rocks -> Arguments.of(mode, rocks)));
  }

  @ParameterizedTest
  @MethodSource("rejectionCases")
  void malformedLaterChunkRejectsTheWholeStockView(String mode, boolean rocks) throws Exception {
    var type = RowType.of(new BigIntType(false), new BigIntType(), new TimestampType(3));
    var keyType = RowType.of(type.getTypeAt(0));
    int[] keys = {0};
    boolean presence = mode.startsWith("marker");
    long handle = create(rocks, keys, false, presence, 0);
    try (var view = harness(type, keys, "proc-last", 0, false)) {
      view.open();
      var backend = backend(view);
      var state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("deduplicate-state", valueSerializer(type, presence)));
      var keyRows = new RowDataSerializer(keyType);
      for (long index = 0; index < 1026; index++) {
        var key = keyRows.toBinaryRow(GenericRowData.of(index)).copy();
        if (index == 1025 && mode.endsWith("key")) key.setRowKind(RowKind.DELETE);
        backend.setCurrentKey(key);
        RowData payload = row(index == 1025 && mode.equals("row-payload") ? -1L : index, 10, 1000);
        if (index == 1025 && mode.equals("row-kind")) payload.setRowKind(RowKind.DELETE);
        state.update(presence ? Boolean.TRUE : payload);
      }
      var contract =
          new StockDedupStateContract(
              new RowDataSerializer(keyType), keyType, type, presence, -1, 0);
      assertThrows(
          RuntimeException.class, () -> contract.importState(ordered(backend), handle, rocks));
      contract.exportState(backend, handle, rocks);
      try (var saved = backend.getKeys("deduplicate-state", VoidNamespace.INSTANCE)) {
        assertEquals(0, saved.count());
      }
    } finally {
      close(handle, rocks);
    }
  }

  @SuppressWarnings("unchecked")
  private static TypeSerializer<Object> valueSerializer(RowType type, boolean presence) {
    return (TypeSerializer<Object>)
        (TypeSerializer<?>) (presence ? BooleanSerializer.INSTANCE : new RowDataSerializer(type));
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> ordered(
      CheckpointableKeyedStateBackend<RowData> backend) {
    return (CheckpointableKeyedStateBackend<RowData>)
        Proxy.newProxyInstance(
            CanonicalEagerDedupInteropTest.class.getClassLoader(),
            new Class<?>[] {CheckpointableKeyedStateBackend.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getKeys"))
                return backend
                    .getKeys((String) args[0], args[1])
                    .sorted(java.util.Comparator.comparingLong(key -> key.getLong(0)));
              try {
                return method.invoke(backend, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
            });
  }

  private long create(boolean rocks, int[] keys, boolean rowtime, boolean first, long retention) {
    int maximum = keys.length == 0 ? 1 : 128;
    int[] precision =
        keys.length == 0
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1};
    return rocks
        ? Native.createRocksDBKeepLastDeduplicator(
            keys,
            precision,
            2,
            true,
            true,
            rowtime,
            first,
            false,
            false,
            retention,
            10000,
            1 << 24,
            directory.resolve("state").toString(),
            maximum,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            0,
            new String[0],
            new String[0],
            0,
            maximum - 1,
            false,
            new byte[0][])
        : Native.createKeepLastDeduplicator(
            keys, precision, 2, true, true, rowtime, first, false, false, retention, 1 << 24);
  }

  private static void close(long handle, boolean rocks) {
    if (rocks) Native.closeRocksDBKeepLastDeduplicator(handle);
    else Native.closeKeepLastDeduplicator(handle);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      RowType type, int[] keys, String mode, long retention, boolean stock) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator;
    if (!stock) operator = new Projection();
    else if (mode.startsWith("row"))
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new RowTimeDeduplicateFunction(
                  InternalTypeInfo.of(type), retention, 2, true, true, mode.endsWith("last")));
    else if (mode.endsWith("first"))
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new ProcTimeDeduplicateKeepFirstRowFunction(retention));
    else
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new ProcTimeDeduplicateKeepLastRowFunction(
                  InternalTypeInfo.of(type),
                  retention,
                  true,
                  true,
                  true,
                  new EqualiserCodeGenerator(
                          type, CanonicalEagerDedupInteropTest.class.getClassLoader())
                      .generateRecordEqualiser("CanonicalDedupEqualiser"),
                  null));
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalEagerDedupInteropTest.class.getClassLoader(), keys, InternalTypeInfo.of(type));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), keys.length == 0 ? 1 : 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(type));
    return harness;
  }

  private static RowData row(Long key, long value, long time) {
    return GenericRowData.of(key, value, TimestampData.fromEpochMillis(time));
  }

  private static List<String> stockPush(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      RowType type,
      List<RowData> rows)
      throws Exception {
    harness.getOutput().clear();
    var serializer = new RowDataSerializer(type);
    for (var row : rows)
      harness.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
    return harness.extractOutputValues().stream()
        .map(CanonicalEagerDedupInteropTest::value)
        .collect(java.util.stream.Collectors.toList());
  }

  private static List<String> nativePush(
      long handle,
      boolean rocks,
      RowType type,
      List<RowData> rows,
      long time,
      boolean insertOnlyOutput) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var input = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var inputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, input, inputSchema);
      if (rocks)
        Native.pushRocksDBKeepLastDeduplicator(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            time,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      else
        Native.pushKeepLastDeduplicator(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            time,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {
        if (result.getRowCount() == 0) return List.of();
        var reader = ArrowConversion.createArrowReader(result, type);
        var kinds = (TinyIntVector) result.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        if (insertOnlyOutput) assertNull(kinds);
        else assertNotNull(kinds);
        var values = new ArrayList<String>();
        for (int i = 0; i < result.getRowCount(); i++) {
          var row = reader.read(i);
          row.setRowKind(insertOnlyOutput ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(i)));
          values.add(value(row));
        }
        return values;
      }
    }
  }

  private static String value(RowData row) {
    return row.getRowKind().shortString()
        + "|"
        + (row.isNullAt(0) ? "null" : row.getLong(0))
        + "|"
        + row.getLong(1)
        + "|"
        + (row.isNullAt(2) ? "null" : row.getTimestamp(2, 3).getMillisecond());
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
