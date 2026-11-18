package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.backend;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockAppendTopNStateContract;

class CanonicalAppendRankMatrixTest {
  @TempDir Path directory;
  static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  static Stream<Arguments> cases() {
    return Stream.of("projected", "hidden", "fast1", "fast1-hidden", "variable")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        stockDisk ->
                            Stream.of(false, true)
                                .flatMap(
                                    nativeDisk ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                ttl ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            keyless ->
                                                                Arguments.of(
                                                                    mode,
                                                                    stockDisk,
                                                                    nativeDisk,
                                                                    ttl,
                                                                    keyless))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void retainedListsAndWinnersContinueThroughBothCanonicalDirections(
      String mode, boolean stockDisk, boolean nativeDisk, boolean ttl, boolean keyless)
      throws Exception {
    boolean variable = mode.equals("variable"), fast = mode.startsWith("fast1");
    boolean rank = !mode.contains("hidden");
    long retention = ttl ? 1000 : 0;
    int[] keys = keyless ? new int[0] : new int[] {0};
    var selector = selector(keys);
    var keyType = (RowType) selector.getProducedType().toLogicalType();
    var contract =
        new StockAppendTopNStateContract(
            new RowDataSerializer(keyType),
            selector,
            selector(new int[] {1}),
            keyType,
            INPUT,
            new int[] {1},
            fast,
            variable,
            ttl);
    long handle = create(mode, nativeDisk, keyless, retention);
    try (var stock = harness(mode, stockDisk, keys, retention, true);
        var view = harness(mode, stockDisk, keys, 0, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1000);
      feed(stock, List.of(row(1L, 20), row(1L, 10), row(1L, 10), row(2L, 30), row(null, 40)));
      if (variable) feed(stock, List.of(GenericRowData.of(9L, 999L, -1L)));
      stock.setStateTtlProcessingTime(1500);
      feed(stock, List.of(row(1L, 15)));
      view.initializeState(snapshot(stock));
      view.open();
      view.setStateTtlProcessingTime(10000);
      contract.importState(backend(view), handle, nativeDisk);
      stock.setStateTtlProcessingTime(1800);
      var suffix = List.of(row(1L, 12), row(2L, 25), row(null, 35), row(4L, 50));
      feed(stock, suffix);
      var output = push(handle, nativeDisk, mode, suffix, 1800);
      assertEquals(values(stock.extractOutputValues(), rank), values(output, rank));
      contract.exportState(backend(view), handle, nativeDisk);
      try (var restored = harness(mode, stockDisk, keys, retention, true)) {
        restored.initializeState(snapshot(view));
        restored.open();
        restored.setStateTtlProcessingTime(1900);
        stock.setStateTtlProcessingTime(1900);
        var future = new ArrayList<RowData>(List.of(row(1L, 9), row(4L, 45)));
        if (variable) future.add(GenericRowData.of(9L, 111L, 2L));
        feed(stock, future);
        feed(restored, future);
        var expected = values(stock.extractOutputValues(), rank);
        assertFalse(expected.isEmpty());
        assertEquals(expected, values(restored.extractOutputValues(), rank));
        assertEquals(expected, values(push(handle, nativeDisk, mode, future, 1900), rank));
      }
    } finally {
      if (nativeDisk) Native.closeRocksDBTopNRanker(handle);
      else Native.closeTopNRanker(handle);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true"
  })
  void fastTop1HonorsTheReleasedBeforeImagePolicyWithNullScores(boolean rocks, boolean before)
      throws Exception {
    String mode = "fast1-hidden";
    long handle = create(mode, rocks, false, 0, before);
    try (var stock = harness(mode, false, new int[] {0}, 0, true, before)) {
      stock.open();
      var rows =
          List.<RowData>of(
              row(2L, 10), row(3L, 20), row(2L, 10), GenericRowData.of(3L, null, 3L), row(2L, 30));
      feed(stock, rows);
      var expected = values(stock.extractOutputValues(), false);
      assertTrue(expected.stream().anyMatch(row -> row.contains("null")));
      assertEquals(expected, values(push(handle, rocks, mode, rows, 0), false));
    } finally {
      if (rocks) Native.closeRocksDBTopNRanker(handle);
      else Native.closeTopNRanker(handle);
    }
  }

  private static RowData row(Long key, long value) {
    return GenericRowData.of(key, value, 3L);
  }

  static RowDataKeySelector selector(int[] keys) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalAppendRankMatrixTest.class.getClassLoader(), keys, InternalTypeInfo.of(INPUT));
  }

  private static RowType output(String mode) {
    return mode.contains("hidden")
        ? INPUT
        : RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      String mode, boolean disk, int[] keys, long ttl, boolean stock) throws Exception {
    return harness(mode, disk, keys, ttl, stock, true);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      String mode, boolean disk, int[] keys, long ttl, boolean stock, boolean before)
      throws Exception {
    OneInputStreamOperator<RowData, RowData> operator = new Projection();
    if (stock) {
      var comparator =
          new GeneratedRecordComparator("", "", new Object[0]) {
            @Override
            public RecordComparator newInstance(ClassLoader loader) {
              return (left, right) -> Long.compare(left.getLong(0), right.getLong(0));
            }
          };
      AbstractTopNFunction function =
          mode.startsWith("fast1")
              ? new FastTop1Function(
                  StateConfigUtil.createTtlConfig(ttl),
                  InternalTypeInfo.of(INPUT),
                  comparator,
                  selector(new int[] {1}),
                  RankType.ROW_NUMBER,
                  new ConstantRankRange(1, 1),
                  before,
                  !mode.contains("hidden"),
                  10000)
              : new AppendOnlyTopNFunction(
                  StateConfigUtil.createTtlConfig(ttl),
                  InternalTypeInfo.of(INPUT),
                  comparator,
                  selector(new int[] {1}),
                  RankType.ROW_NUMBER,
                  mode.equals("variable") ? new VariableRankRange(2) : new ConstantRankRange(1, 3),
                  true,
                  !mode.contains("hidden"),
                  10000);
      var process = new KeyedProcessOperator<RowData, RowData, RowData>(function);
      function.setKeyContext(process);
      operator = process;
    }
    var selector = selector(keys);
    var h =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), keys.length == 0 ? 1 : 128, 1, 0);
    if (disk) h.setStateBackend(tech.streamfusion.compat.CanonicalTestCompat.rocksBackend());
    else h.setStateBackend(new HashMapStateBackend());
    h.setup(new RowDataSerializer(output(mode)));
    return h;
  }

  private static void feed(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, List<RowData> rows)
      throws Exception {
    h.getOutput().clear();
    var serializer = new RowDataSerializer(INPUT);
    for (var row : rows) h.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
  }

  private long create(String mode, boolean rocks, boolean keyless, long ttl) {
    return create(mode, rocks, keyless, ttl, true);
  }

  private long create(String mode, boolean rocks, boolean keyless, long ttl, boolean before) {
    int[] keys = keyless ? new int[0] : new int[] {0};
    int[] descriptor =
        keyless
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1};
    long limit = mode.startsWith("fast1") ? 1 : 3;
    boolean rank = !mode.contains("hidden");
    int bound = mode.equals("variable") ? 2 : -1;
    long handle;
    if (!rocks)
      handle =
          Native.createTopNRanker(
              keys,
              descriptor,
              new int[] {1},
              new int[] {1},
              new int[] {1},
              0,
              limit,
              bound,
              rank,
              false,
              before,
              false,
              ttl,
              1 << 24);
    else
      try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        try {
          Data.exportSchema(
              NativeAllocator.SHARED,
              ArrowConversion.toArrowSchema(INPUT),
              NativeAllocator.DICTIONARIES,
              schema);
          int maximum = keyless ? 1 : 128;
          handle =
              Native.createRocksDBTopNRanker(
                  keys,
                  descriptor,
                  new int[] {1},
                  new int[] {1},
                  new int[] {1},
                  0,
                  limit,
                  bound,
                  rank,
                  false,
                  before,
                  false,
                  ttl,
                  0,
                  1 << 24,
                  schema.memoryAddress(),
                  directory.resolve("state").toString(),
                  maximum,
                  FlinkRocksDBOptions.from(new Configuration()).json(),
                  0,
                  new String[0],
                  new String[0],
                  0,
                  maximum - 1,
                  false,
                  new byte[0][]);
        } finally {
          if (schema.snapshot().release != 0) schema.release();
        }
      }
    if (mode.equals("variable")) Native.enableTopNFirstBound(handle, rocks, true);
    return handle;
  }

  private static List<RowData> push(
      long handle, boolean rocks, String mode, List<RowData> rows, long now) {
    try (var root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var out = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (rocks)
        Native.pushRocksDBTopNRanker(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            now,
            out.memoryAddress(),
            outSchema.memoryAddress());
      else
        Native.pushTopNRanker(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            now,
            out.memoryAddress(),
            outSchema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, out, outSchema, NativeAllocator.DICTIONARIES)) {
        if (result.getRowCount() == 0) return List.of();
        var values = new ArrayList<RowData>();
        var kinds =
            (org.apache.arrow.vector.TinyIntVector)
                result.getVector(result.getFieldVectors().size() - 1);
        var reader = ArrowConversion.createArrowReader(result, output(mode));
        var serializer = new RowDataSerializer(output(mode));
        for (int i = 0; i < result.getRowCount(); i++) {
          var row = serializer.copy(reader.read(i));
          row.setRowKind(org.apache.flink.types.RowKind.fromByteValue(kinds.get(i)));
          values.add(row);
        }
        return values;
      }
    }
  }

  private static List<String> values(List<RowData> rows, boolean rank) {
    return rows.stream()
        .map(
            row ->
                row.getRowKind()
                    + ":"
                    + (row.isNullAt(0) ? "null" : row.getLong(0))
                    + ":"
                    + (row.isNullAt(1) ? "null" : row.getLong(1))
                    + ":"
                    + row.getLong(2)
                    + (rank ? ":" + row.getLong(3) : ""))
        .collect(java.util.stream.Collectors.toList());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void exportRejectsANullPrimitiveInsteadOfFabricatingAStockValue(boolean rocks) throws Exception {
    long handle = create("projected", rocks, false, 0);
    try (var view = harness("projected", false, new int[] {0}, 0, false)) {
      view.open();
      push(handle, rocks, "projected", List.of(GenericRowData.of(1L, null, 3L)), 1000);
      RowType strict = RowType.of(new BigIntType(), new BigIntType(false), new BigIntType());
      var key =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(strict));
      var sort =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {1}, InternalTypeInfo.of(strict));
      var keyType = (RowType) key.getProducedType().toLogicalType();
      var contract =
          new StockAppendTopNStateContract(
              new RowDataSerializer(keyType),
              key,
              sort,
              keyType,
              strict,
              new int[] {1},
              false,
              false,
              false);
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> contract.exportState(backend(view), handle, rocks));
      assertTrue(failure.toString().contains("project append"), failure.toString());
      assertTrue(failure.getCause().getMessage().contains("NULL non-null"), failure.toString());
      var count = new java.util.concurrent.atomic.AtomicInteger();
      visit(
          handle,
          rocks,
          root -> {
            assertTrue(root.getVector(1).isNull(0));
            count.addAndGet(root.getRowCount());
          });
      assertEquals(1, count.get());
    } finally {
      if (rocks) Native.closeRocksDBTopNRanker(handle);
      else Native.closeTopNRanker(handle);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void receiverFailureReleasesArrowBuffersAndLeavesStateRetryable(boolean rocks) throws Exception {
    long handle = create("projected", rocks, false, 0);
    try (var view = harness("projected", false, new int[] {0}, 0, false);
        var restored = harness("projected", false, new int[] {0}, 0, true)) {
      view.open();
      var rows = new ArrayList<RowData>();
      for (long key = 0; key < 1025; key++) rows.add(row(key, 10));
      push(handle, rocks, "projected", rows, 1000);
      var marker = new IllegalStateException("receiver marker");
      long before = NativeAllocator.SHARED.getAllocatedMemory();
      assertSame(
          marker,
          assertThrows(
              IllegalStateException.class,
              () ->
                  visit(
                      handle,
                      rocks,
                      root -> {
                        assertTrue(root.getRowCount() <= 1024);
                        throw marker;
                      })));
      assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
      var count = new java.util.concurrent.atomic.AtomicInteger();
      visit(handle, rocks, root -> count.addAndGet(root.getRowCount()));
      assertEquals(1025, count.get());
      var selector = selector(new int[] {0});
      var keyType = (RowType) selector.getProducedType().toLogicalType();
      var contract =
          new StockAppendTopNStateContract(
              new RowDataSerializer(keyType),
              selector,
              selector(new int[] {1}),
              keyType,
              INPUT,
              new int[] {1},
              false,
              false,
              false);
      contract.exportState(backend(view), handle, rocks);
      restored.initializeState(snapshot(view));
      restored.open();
      var future = List.of(row(0L, 5), row(1024L, 6), row(1026L, 7));
      feed(restored, future);
      assertEquals(
          values(restored.extractOutputValues(), true),
          values(push(handle, rocks, "projected", future, 1500), true));
    } finally {
      if (rocks) Native.closeRocksDBTopNRanker(handle);
      else Native.closeTopNRanker(handle);
    }
  }

  private static void visit(
      long handle, boolean rocks, java.util.function.Consumer<VectorSchemaRoot> receiver) {
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
        Native.exportAppendTopNCanonical(
            handle,
            rocks,
            schema.memoryAddress(),
            (arrayAddress, schemaAddress) -> {
              try (var array = ArrowArray.wrap(arrayAddress);
                  var exported = ArrowSchema.wrap(schemaAddress);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, exported, NativeAllocator.DICTIONARIES)) {
                receiver.accept(root);
              }
            });
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> value) {}
  }
}
