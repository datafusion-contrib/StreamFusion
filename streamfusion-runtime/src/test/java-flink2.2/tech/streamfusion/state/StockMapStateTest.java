package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StockMapStateTest {
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(rocks -> Stream.of(false, true).map(ttl -> Arguments.of(rocks, ttl)));
  }

  private static BinaryRowData key() {
    var key = new BinaryRowData(1);
    var writer = new BinaryRowWriter(key);
    writer.writeLong(0, 1L);
    writer.complete();
    return key;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      Function function, boolean rocks) throws Exception {
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new KeyedProcessOperator<>(function),
            row -> key(),
            InternalTypeInfo.of(KEY),
            128,
            1,
            0);
    harness.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(INPUT));
    return harness;
  }

  @ParameterizedTest
  @MethodSource("cases")
  void directProjectionPreservesExpiredEntriesAgesAndStockCanonicalRestore(
      boolean rocks, boolean ttl) throws Exception {
    var function = new Function(ttl);
    try (var source = harness(function, rocks)) {
      source.open();
      source.setStateTtlProcessingTime(1000);
      source.processElement(new StreamRecord<>(GenericRowData.of(1L, -1L)));
      source.setStateTtlProcessingTime(1500);
      source.processElement(new StreamRecord<>(GenericRowData.of(2L, 3L)));
      source.setStateTtlProcessingTime(2200);
      source.prepareSnapshotPreBarrier(1);
      var saved =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var projectionHarness = harness(new Function(ttl, true), rocks)) {
        projectionHarness.initializeState(saved);
        projectionHarness.open();
        projectionHarness.setStateTtlProcessingTime(2200);
        var backend =
            (CheckpointableKeyedStateBackend<RowData>)
                projectionHarness.getOperator().<RowData>getKeyedStateBackend();
        backend.setCurrentKey(key());
        var projection =
            new StockMapState<>(
                backend, "views", LongSerializer.INSTANCE, LongSerializer.INSTANCE, ttl);
        var expected = Map.of(1L, "-1:" + (ttl ? 1000 : 0), 2L, "3:" + (ttl ? 1500 : 0));
        assertEquals(expected, values(projection));
        projection.put(7L, -5L, ttl ? 1700 : 0);
        var projected = new java.util.HashMap<>(expected);
        projected.put(7L, "-5:" + (ttl ? 1700 : 0));
        assertEquals(projected, values(projection));
        projectionHarness.prepareSnapshotPreBarrier(2);
        var exported =
            projectionHarness
                .snapshotWithLocalState(
                    2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState();
        projection.clear();
        assertTrue(values(projection).isEmpty());
        var continued = new Function(ttl);
        try (var restored = harness(continued, rocks)) {
          restored.initializeState(exported);
          restored.open();
          restored.setStateTtlProcessingTime(2200);
          restored.getOperator().<RowData>getKeyedStateBackend().setCurrentKey(key());
          assertEquals(ttl ? null : Long.valueOf(-1), continued.state.get(1L));
          assertEquals(Long.valueOf(3), continued.state.get(2L));
          assertEquals(Long.valueOf(-5), continued.state.get(7L));
          restored.setStateTtlProcessingTime(2700);
          assertEquals(ttl ? null : Long.valueOf(-5), continued.state.get(7L));
        }
      }
    }
  }

  private static Map<Long, String> values(StockMapState<Long, Long> state) throws Exception {
    var result = new java.util.HashMap<Long, String>();
    for (Map.Entry<Long, TtlValue<Long>> entry : state.entries())
      result.put(
          entry.getKey(),
          entry.getValue().getUserValue() + ":" + entry.getValue().getLastAccessTimestamp());
    return result;
  }

  static class Function extends KeyedProcessFunction<RowData, RowData, RowData> {
    private final boolean ttl;
    private final boolean projection;
    transient MapState<Long, Long> state;

    Function(boolean ttl) {
      this(ttl, false);
    }

    Function(boolean ttl, boolean projection) {
      this.ttl = ttl;
      this.projection = projection;
    }

    @Override
    public void open(OpenContext context) throws Exception {
      if (projection) return;
      var descriptor =
          new MapStateDescriptor<>("views", LongSerializer.INSTANCE, LongSerializer.INSTANCE);
      if (ttl)
        descriptor.enableTimeToLive(
            StateTtlConfig.newBuilder(Duration.ofMillis(1000))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .disableCleanupInBackground()
                .build());
      state = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(RowData row, Context context, Collector<RowData> output)
        throws Exception {
      state.put(row.getLong(0), row.getLong(1));
    }
  }
}
