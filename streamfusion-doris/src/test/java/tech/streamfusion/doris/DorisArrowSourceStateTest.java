package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.SortedMap;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.RowDataDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class DorisArrowSourceStateTest {
  @Test
  void allScanModesKeepTheStockBoundednessAndSerializers() throws Exception {
    for (var mode : DorisSourceScanMode.values()) {
      var reads = reads(mode, false);
      var stock = stock(reads);
      var arrow = new DorisArrowSource(stock, options(), reads, type());
      assertEquals(stock.getBoundedness(), arrow.getBoundedness());
      var split = DorisStreamSplit.of("2026-10-22 00:00:00", "2026-10-22 00:00:10");
      var serializer = arrow.getSplitSerializer();
      assertEquals(
          split, serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
      assertEquals(
          stock.getEnumeratorCheckpointSerializer().getClass(),
          arrow.getEnumeratorCheckpointSerializer().getClass());
    }
  }

  @Test
  void checkpointsOnlyPublishFinishedWindowsAndOnlyFromSubtaskZero() throws Exception {
    var reads = reads(DorisSourceScanMode.INITIAL, true);
    var arrow =
        new DorisArrowSource(
            stock(reads(DorisSourceScanMode.INITIAL, false)), options(), reads, type());
    var split = DorisStreamSplit.of("2026-10-22 00:00:00", "2026-10-22 00:00:10");
    for (int subtask = 0; subtask < 2; subtask++) {
      try (var reader = arrow.new Reader(context(subtask))) {
        reader.snapshotState(1L);
        assertTrue(
            offsets(reader).isEmpty(), "An unfinished window must not advance external progress");
        reader.onSplitFinished(Map.of(split.splitId(), split));
        assertTrue(offsets(reader).isEmpty(), "Completion alone is not a committed checkpoint");
        reader.snapshotState(2L);
        assertEquals(
            subtask == 0 ? Map.of(2L, split.getEndTimestamp()) : Map.of(), offsets(reader));
        reader.snapshotState(3L);
        assertEquals(
            subtask == 0 ? 2 : 0,
            offsets(reader).size(),
            "A later checkpoint can retry publication");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static SortedMap<Long, String> offsets(Object reader) throws Exception {
    var field = reader.getClass().getDeclaredField("offsets");
    field.setAccessible(true);
    return (SortedMap<Long, String>) field.get(reader);
  }

  private static SourceReaderContext context(int subtask) {
    return (SourceReaderContext)
        Proxy.newProxyInstance(
            SourceReaderContext.class.getClassLoader(),
            new Class<?>[] {SourceReaderContext.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getConfiguration":
                  return new Configuration();
                case "metricGroup":
                  return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
                case "getIndexOfSubtask":
                  return subtask;
                case "currentParallelism":
                  return 2;
                case "getUserCodeClassLoader":
                  return null;
                default:
                  return null;
              }
            });
  }

  private static RowType type() {
    return RowType.of(new IntType());
  }

  private static DorisOptions options() {
    return DorisOptions.builder()
        .setFenodes("127.0.0.1:8030")
        .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("")
        .build();
  }

  private static DorisReadOptions reads(DorisSourceScanMode mode, boolean publish) {
    var builder = DorisReadOptions.builder().setUseFlightSql(true).setScanMode(mode);
    if (mode == DorisSourceScanMode.FROM_TIMESTAMP) builder.setScanTimestamp("2026-10-22 00:00:00");
    if (publish) builder.setBinlogOffsetTable("db.offsets").setBinlogConsumerId("consumer");
    return builder.build();
  }

  private static DorisSource<?> stock(DorisReadOptions reads) {
    return DorisSource.<org.apache.flink.table.data.RowData>builder()
        .setDorisOptions(options())
        .setDorisReadOptions(reads)
        .setDeserializer(new RowDataDeserializationSchema(type()))
        .build();
  }
}
