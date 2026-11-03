package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockRetractingTopNStateContract;

class CanonicalRetractingRankStateInteropTest {
  static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType KEY = RowType.of(new BigIntType());
  @TempDir java.nio.file.Path directory;

  static Stream<Arguments> traces() {
    return Stream.of("offset", "missing", "empty", "ttl", "ttl-offset", "bound", "bound-ttl")
        .flatMap(
            shape ->
                Stream.of("heap", "rocks")
                    .flatMap(
                        store ->
                            Stream.of("hidden", "rank")
                                .flatMap(
                                    rank ->
                                        Stream.of(
                                            Arguments.of(
                                                shape + "-" + store + "-" + rank, false, false),
                                            Arguments.of(
                                                shape + "-" + store + "-" + rank, true, false),
                                            Arguments.of(
                                                shape + "-" + store + "-" + rank, false, true)))));
  }

  @ParameterizedTest(name = "{0}, native-checkpoint-before-canonical={1}, native-first={2}")
  @MethodSource("traces")
  void stockCanonicalStateAndNativeCheckpointsContinueThroughDirectCanonicalTransfer(
      String name, boolean raw, boolean nativeFirst) throws Exception {
    JsonNode trace;
    try (var source = getClass().getResourceAsStream("/retracting-rank/" + name + ".json")) {
      assertNotNull(source);
      trace = new ObjectMapper().readTree(source);
    }
    boolean persistent = trace.path("serializes_values").asBoolean();
    boolean rank = trace.path("output_rank").asBoolean();
    var type = rank ? OUTPUT : INPUT;
    long ttl = trace.path("ttl_ms").asLong();
    long start = trace.path("rank_start").asLong();
    boolean variable = trace.path("variable_bound").asBoolean();
    var steps = trace.path("steps");
    int restoreAfter = trace.path("restore_after").asInt();
    OperatorSubtaskState prefix;
    try (var stock =
        RetractingRankStockStateTest.harness(true, persistent, rank, ttl, start, variable)) {
      stock.open();
      for (int index = 0; index < restoreAfter; index++) {
        var step = steps.get(index);
        RetractingRankStockStateTest.time(stock, step.path("now").asLong());
        RetractingRankStockStateTest.push(stock, row(step));
      }
      prefix = RetractingRankStockStateTest.snapshot(stock);
    }
    long handle = create(raw ? false : persistent, trace, new byte[0][], 0);
    boolean handlePersistent = raw ? false : persistent;
    try (var projection = RetractingRankStockStateTest.harness(false, persistent, rank, 0);
        var resumed =
            RetractingRankStockStateTest.harness(true, persistent, rank, ttl, start, variable)) {
      projection.initializeState(prefix);
      projection.open();
      var original =
          RetractingRankStockStateTest.timedStateView(
              RetractingRankStockStateTest.backend(projection), ttl > 0, variable);
      var contract = contract(trace);
      if (nativeFirst) {
        for (int index = 0; index < restoreAfter; index++) {
          var step = steps.get(index);
          assertStep(
              step,
              push(handle, handlePersistent, List.of(row(step)), rank, step.path("now").asLong()),
              type);
        }
      } else {
        contract.importState(
            RetractingRankStockStateTest.backend(projection), handle, handlePersistent);
      }
      if (raw) {
        byte[][] snapshots =
            Native.snapshotTopNRankerPartitions(
                handle, 128, new int[] {Native.STOCK_KEY_PROJECTION, -1});
        assertTrue(snapshots.length > 0, "Count-only state must own a native key group");
        for (int i = 0; i < snapshots.length; i++)
          snapshots[i] = java.util.Arrays.copyOfRange(snapshots[i], 4, snapshots[i].length);
        close(handle, false);
        handle = 0;
        handle = create(persistent, trace, snapshots, 5000);
        handlePersistent = persistent;
      }
      contract.exportState(
          RetractingRankStockStateTest.backend(projection), handle, handlePersistent);
      assertEquals(
          original,
          RetractingRankStockStateTest.timedStateView(
              RetractingRankStockStateTest.backend(projection), ttl > 0, variable),
          "No-input re-export must preserve complete independent state");
      var snapshot = RetractingRankStockStateTest.snapshot(projection);
      resumed.initializeState(snapshot);
      resumed.open();
      for (int index = restoreAfter; index < steps.size(); index++) {
        var step = steps.get(index);
        var input = row(step);
        long now = step.path("now").asLong();
        RetractingRankStockStateTest.time(resumed, now);
        resumed.getOutput().clear();
        RetractingRankStockStateTest.push(resumed, input);
        var expected = resumed.extractOutputValues();
        var nativeOutput = push(handle, handlePersistent, List.of(input), rank, now);
        assertEquals(bytes(type, expected), bytes(type, nativeOutput), name + " step " + index);
        assertStep(step, expected, type);
      }
    } finally {
      if (handle != 0) close(handle, handlePersistent);
    }
  }

  private static void assertStep(JsonNode step, List<RowData> rows, RowType type) throws Exception {
    var actual = new ArrayList<Object>();
    for (var row : rows) {
      var fields = new ArrayList<Long>();
      for (int column = 0; column < type.getFieldCount(); column++) fields.add(row.getLong(column));
      actual.add(java.util.Map.of("kind", row.getRowKind().toByteValue(), "fields", fields));
    }
    var json = new ObjectMapper();
    assertEquals(step.path("output"), json.readTree(json.writeValueAsBytes(actual)));
  }

  static StockRetractingTopNStateContract contract(JsonNode trace) {
    return new StockRetractingTopNStateContract(
        new RowDataSerializer(KEY),
        KeySelectorUtil.getRowDataSelector(
            CanonicalRetractingRankStateInteropTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(INPUT)),
        KeySelectorUtil.getRowDataSelector(
            CanonicalRetractingRankStateInteropTest.class.getClassLoader(),
            new int[] {1},
            InternalTypeInfo.of(INPUT)),
        KEY,
        INPUT,
        RetractingRankStockStateTest.comparator(),
        trace.path("variable_bound").asBoolean(),
        trace.path("ttl_ms").asLong() > 0);
  }

  private static RowData row(JsonNode step) {
    var values = step.path("input").path("fields");
    var row =
        GenericRowData.of(values.get(0).asLong(), values.get(1).asLong(), values.get(2).asLong());
    row.setRowKind(RowKind.fromByteValue((byte) step.path("input").path("kind").asInt()));
    return row;
  }

  long create(boolean persistent, JsonNode trace, byte[][] snapshots, long restoredAt)
      throws Exception {
    return create(persistent, trace, snapshots, restoredAt, 1 << 24);
  }

  long create(boolean persistent, JsonNode trace, byte[][] snapshots, long restoredAt, long budget)
      throws Exception {
    int[] partition = {0}, precision = {Native.STOCK_KEY_PROJECTION, -1}, sort = {1}, order = {1};
    long offset = trace.path("rank_start").asLong() - 1, limit = trace.path("rank_end").asLong();
    boolean rank = trace.path("output_rank").asBoolean(),
        variable = trace.path("variable_bound").asBoolean();
    long ttl = trace.path("ttl_ms").asLong();
    int bound = variable ? 2 : -1;
    long handle;
    if (!persistent) {
      handle =
          snapshots.length == 0
              ? Native.createTopNRanker(
                  partition, precision, sort, order, order, offset, limit, bound, rank, true, true,
                  false, ttl, budget)
              : Native.restoreTopNRankerPartitions(
                  partition,
                  precision,
                  sort,
                  order,
                  order,
                  offset,
                  limit,
                  bound,
                  rank,
                  true,
                  true,
                  false,
                  ttl,
                  restoredAt,
                  snapshots,
                  budget);
    } else {
      try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
          AutoCloseable release =
              () -> {
                if (schema.snapshot().release != 0) schema.release();
              }) {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
        handle =
            Native.createRocksDBTopNRanker(
                partition,
                precision,
                sort,
                order,
                order,
                offset,
                limit,
                bound,
                rank,
                true,
                true,
                false,
                ttl,
                restoredAt,
                budget,
                schema.memoryAddress(),
                directory.resolve("state-" + java.util.UUID.randomUUID()).toString(),
                128,
                FlinkRocksDBOptions.from(new Configuration()).json(),
                0,
                new String[0],
                new String[0],
                0,
                127,
                false,
                snapshots);
      }
    }
    if (variable) Native.enableTopNFirstBound(handle, persistent, true);
    Native.enableStockRetractingTopNState(handle, persistent);
    return handle;
  }

  static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBTopNRanker(handle);
    else Native.closeTopNRanker(handle);
  }

  static List<RowData> push(
      long handle, boolean persistent, List<RowData> rows, boolean rank, long now) {
    try (var root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED, true);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var out = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (persistent)
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
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, out, outSchema, NativeAllocator.DICTIONARIES)) {
        if (result.getRowCount() == 0) return List.of();
        var type = rank ? OUTPUT : INPUT;
        var reader = ArrowConversion.createArrowReader(result, type);
        var serializer = new RowDataSerializer(type);
        var values = new ArrayList<RowData>();
        var kinds = (org.apache.arrow.vector.TinyIntVector) result.getVector(type.getFieldCount());
        for (int index = 0; index < result.getRowCount(); index++) {
          var row = serializer.copy(reader.read(index));
          row.setRowKind(RowKind.fromByteValue(kinds.get(index)));
          values.add(row);
        }
        return values;
      }
    }
  }
}
