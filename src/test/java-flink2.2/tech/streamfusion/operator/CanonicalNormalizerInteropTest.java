package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.backend;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockChangelogNormalizeStateContract;

class CanonicalNormalizerInteropTest {
  private static final RowType TYPE = RowType.of(new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void releasedRowsAndAgesContinueAcrossBothSwitches(boolean rocks, boolean ttl) throws Exception {
    long handle = create(rocks, ttl);
    var prefix =
        List.<RowData>of(
            row(RowKind.INSERT, 1L, 10L),
            row(RowKind.UPDATE_AFTER, 1L, 15L),
            row(RowKind.INSERT, 2L, 20L),
            row(RowKind.INSERT, null, 30L));
    var suffix =
        List.<RowData>of(
            row(RowKind.UPDATE_AFTER, 1L, 15L),
            row(RowKind.DELETE, 2L, null),
            row(RowKind.INSERT, 3L, 30L),
            row(RowKind.UPDATE_AFTER, 1L, 18L));
    var future =
        List.<RowData>of(
            row(RowKind.DELETE, 1L, null),
            row(RowKind.UPDATE_AFTER, 3L, 35L),
            row(RowKind.INSERT, 4L, 40L));
    try (var stock = harness(true, ttl);
        var view = harness(false, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1500);
      stockPush(stock, prefix);
      view.initializeState(snapshot(stock));
      view.open();
      view.setStateTtlProcessingTime(10000);
      var contract =
          new StockChangelogNormalizeStateContract(new RowDataSerializer(KEY), KEY, TYPE, ttl);
      contract.importState(backend(view), handle, rocks);
      stock.setStateTtlProcessingTime(1800);
      assertEquals(stockPush(stock, suffix), nativePush(handle, rocks, suffix, 1800));
      contract.exportState(backend(view), handle, rocks);
      try (var restored = harness(true, ttl)) {
        restored.initializeState(snapshot(view));
        restored.open();
        restored.setStateTtlProcessingTime(1900);
        stock.setStateTtlProcessingTime(1900);
        var expected = stockPush(stock, future);
        assertEquals(expected, stockPush(restored, future));
        assertEquals(expected, nativePush(handle, rocks, future, 1900));
      }
    } finally {
      close(handle, rocks);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void lateBadStockRowRejectsTheWholeView(boolean rocks, boolean mismatchedKey) throws Exception {
    long handle = create(rocks, false);
    try (var view = harness(false, false)) {
      view.open();
      var backend = backend(view);
      var state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("deduplicate-state", new RowDataSerializer(TYPE)));
      var keyRows = new RowDataSerializer(KEY);
      for (long index = 0; index < 1026; index++) {
        backend.setCurrentKey(keyRows.toBinaryRow(GenericRowData.of(index)).copy());
        state.update(
            row(
                index == 1025 && !mismatchedKey ? RowKind.DELETE : RowKind.INSERT,
                index == 1025 && mismatchedKey ? -1L : index,
                10L));
      }
      var ordered = ordered(backend);
      var contract =
          new StockChangelogNormalizeStateContract(new RowDataSerializer(KEY), KEY, TYPE, false);
      assertThrows(RuntimeException.class, () -> contract.importState(ordered, handle, rocks));
      contract.exportState(backend, handle, rocks);
      try (var keys = backend.getKeys("deduplicate-state", VoidNamespace.INSTANCE)) {
        assertEquals(0, keys.count());
      }
    } finally {
      close(handle, rocks);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void nonemptyTargetRejectsWithoutReplacingItsRows(boolean rocks) throws Exception {
    long handle = create(rocks, false);
    try (var view = harness(false, false)) {
      view.open();
      assertEquals(
          List.of("+I|1025|10"),
          nativePush(handle, rocks, List.of(row(RowKind.INSERT, 1025L, 10L)), 1500));
      var contract =
          new StockChangelogNormalizeStateContract(new RowDataSerializer(KEY), KEY, TYPE, false);
      assertThrows(
          RuntimeException.class, () -> contract.importState(backend(view), handle, rocks));
      assertEquals(
          List.of("-D|1025|10"),
          nativePush(handle, rocks, List.of(row(RowKind.DELETE, 1025L, null)), 1500));
    } finally {
      close(handle, rocks);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void rawAgesSurviveDowntimeAndExpireAtTheOriginalBoundary(boolean rocks) throws Exception {
    long handle = create(rocks, true);
    try (var stock = harness(true, true);
        var view = harness(false, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1000);
      stockPush(stock, List.of(row(RowKind.INSERT, 1L, 10L)));
      stock.setStateTtlProcessingTime(1500);
      stockPush(stock, List.of(row(RowKind.INSERT, 2L, 20L)));
      view.initializeState(snapshot(stock));
      view.open();
      view.setStateTtlProcessingTime(10000);
      var contract =
          new StockChangelogNormalizeStateContract(new RowDataSerializer(KEY), KEY, TYPE, true);
      var backend = backend(view);
      contract.importState(backend, handle, rocks);
      contract.exportState(backend, handle, rocks);
      var raw =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "deduplicate-state",
                  new org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer<>(
                      org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
                      new RowDataSerializer(TYPE))));
      var keyRows = new RowDataSerializer(KEY);
      backend.setCurrentKey(keyRows.toBinaryRow(GenericRowData.of(1L)).copy());
      assertEquals(1000, raw.value().getLastAccessTimestamp());
      backend.setCurrentKey(keyRows.toBinaryRow(GenericRowData.of(2L)).copy());
      assertEquals(1500, raw.value().getLastAccessTimestamp());
      try (var restored = harness(true, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        restored.setStateTtlProcessingTime(2000);
        var deletes =
            List.<RowData>of(row(RowKind.DELETE, 1L, null), row(RowKind.DELETE, 2L, null));
        assertEquals(List.of("-D|2|20"), stockPush(restored, deletes));
        assertEquals(List.of("-D|2|20"), nativePush(handle, rocks, deletes, 2000));
      }
    } finally {
      close(handle, rocks);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void boundedTransfersPreserveRowsPastTwoChunkBoundaries(boolean rocks) throws Exception {
    long handle = create(rocks, false);
    try (var stock = harness(true, false);
        var view = harness(false, false)) {
      stock.open();
      var input = new ArrayList<RowData>();
      for (long key = 0; key < 2049; key++) input.add(row(RowKind.INSERT, key, key + 10));
      stockPush(stock, input);
      view.initializeState(snapshot(stock));
      view.open();
      var contract =
          new StockChangelogNormalizeStateContract(new RowDataSerializer(KEY), KEY, TYPE, false);
      contract.importState(backend(view), handle, rocks);
      var chunks = new ArrayList<Integer>();
      scan(
          handle,
          rocks,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              assertTrue(root.getRowCount() <= 1024);
              chunks.add(root.getRowCount());
            }
          });
      assertEquals(List.of(1024, 1024, 1), chunks);
      contract.exportState(backend(view), handle, rocks);
      try (var restored = harness(true, false)) {
        restored.initializeState(snapshot(view));
        restored.open();
        var deletes =
            List.<RowData>of(
                row(RowKind.DELETE, 0L, null),
                row(RowKind.DELETE, 1024L, null),
                row(RowKind.DELETE, 2048L, null));
        var expected = List.of("-D|0|10", "-D|1024|1034", "-D|2048|2058");
        assertEquals(expected, stockPush(restored, deletes));
        assertEquals(expected, nativePush(handle, rocks, deletes, 0));
      }
    } finally {
      close(handle, rocks);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void receiverFailureLeavesRowsAvailableForExportRetry(boolean rocks) throws Exception {
    long handle = create(rocks, false);
    try (var view = harness(false, false)) {
      view.open();
      nativePush(handle, rocks, List.of(row(RowKind.INSERT, 1L, 10L)), 0);
      assertThrows(
          RuntimeException.class,
          () ->
              scan(
                  handle,
                  rocks,
                  (arrayAddress, schemaAddress) -> {
                    try (var array = ArrowArray.wrap(arrayAddress);
                        var schema = ArrowSchema.wrap(schemaAddress);
                        var root =
                            Data.importVectorSchemaRoot(
                                NativeAllocator.SHARED,
                                array,
                                schema,
                                NativeAllocator.DICTIONARIES)) {
                      assertEquals(1, root.getRowCount());
                      throw new IllegalStateException("normalizer receiver failed");
                    }
                  }));
      new StockChangelogNormalizeStateContract(new RowDataSerializer(KEY), KEY, TYPE, false)
          .exportState(backend(view), handle, rocks);
      try (var restored = harness(true, false)) {
        restored.initializeState(snapshot(view));
        restored.open();
        assertEquals(
            List.of("-D|1|10"), stockPush(restored, List.of(row(RowKind.DELETE, 1L, null))));
      }
      assertEquals(
          List.of("-D|1|10"), nativePush(handle, rocks, List.of(row(RowKind.DELETE, 1L, null)), 0));
    } finally {
      close(handle, rocks);
    }
  }

  private static void scan(long handle, boolean rocks, Native.BatchReceiver receiver) {
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(TYPE),
            NativeAllocator.DICTIONARIES,
            schema);
        Native.exportChangelogNormalizerCanonical(handle, rocks, schema.memoryAddress(), receiver);
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  private long create(boolean rocks, boolean ttl) {
    int[] precision = {Native.STOCK_KEY_PROJECTION, -1};
    return rocks
        ? Native.createRocksDBChangelogNormalizer(
            new int[] {0},
            precision,
            true,
            false,
            ttl ? 1000 : 0,
            10000,
            1 << 24,
            directory.resolve("state").toString(),
            128,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            0,
            new String[0],
            new String[0],
            0,
            127,
            false,
            new byte[0][])
        : Native.createChangelogNormalizer(
            new int[] {0}, precision, true, false, ttl ? 1000 : 0, 1 << 24);
  }

  private static void close(long handle, boolean rocks) {
    if (rocks) Native.closeRocksDBChangelogNormalizer(handle);
    else Native.closeChangelogNormalizer(handle);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock, boolean ttl) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator;
    if (stock) {
      var equaliser =
          new GeneratedRecordEqualiser("", "", new Object[0]) {
            @Override
            public RecordEqualiser newInstance(ClassLoader loader) {
              return (left, right) ->
                  java.util.stream.IntStream.range(0, 2)
                      .allMatch(
                          i ->
                              left.isNullAt(i) == right.isNullAt(i)
                                  && (left.isNullAt(i) || left.getLong(i) == right.getLong(i)));
            }
          };
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new ProcTimeDeduplicateKeepLastRowFunction(
                  InternalTypeInfo.of(TYPE), ttl ? 1000 : 0, true, true, false, equaliser, null));
    } else operator = new Projection();
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalNormalizerInteropTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(TYPE));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(TYPE));
    return harness;
  }

  private static RowData row(RowKind kind, Long key, Long value) {
    var row = GenericRowData.of(key, value);
    row.setRowKind(kind);
    return row;
  }

  private static List<String> stockPush(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, List<RowData> rows)
      throws Exception {
    harness.getOutput().clear();
    var serializer = new RowDataSerializer(TYPE);
    for (var row : rows)
      harness.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
    return harness.extractOutputValues().stream()
        .map(CanonicalNormalizerInteropTest::value)
        .collect(java.util.stream.Collectors.toList());
  }

  private static List<String> nativePush(
      long handle, boolean rocks, List<RowData> rows, long time) {
    try (var root = RowDataArrowConverter.write(rows, TYPE, NativeAllocator.SHARED, true);
        var input = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var inputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, input, inputSchema);
      if (rocks)
        Native.pushRocksDBChangelogNormalizer(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            time,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      else
        Native.pushChangelogNormalizer(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            time,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {
        var reader = ArrowConversion.createArrowReader(result, TYPE);
        var kinds = (TinyIntVector) result.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        var values = new ArrayList<String>();
        for (int i = 0; i < result.getRowCount(); i++) {
          var row = reader.read(i);
          row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
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
        + (row.isNullAt(1) ? "null" : row.getLong(1));
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> ordered(
      CheckpointableKeyedStateBackend<RowData> backend) {
    return (CheckpointableKeyedStateBackend<RowData>)
        Proxy.newProxyInstance(
            CanonicalNormalizerInteropTest.class.getClassLoader(),
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

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
