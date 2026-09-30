package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonSourceRecoveryTest {
  @ParameterizedTest
  @CsvSource({
    "false,default",
    "true,default",
    "false,sequence",
    "true,sequence",
    "false,first-row",
    "true,first-row",
    "false,partial-update",
    "true,partial-update"
  })
  void checkpointOnlyAdvancesAfterEmissionAndRestoresInsideBatch(boolean tail, String mode)
      throws Exception {
    var options = new java.util.HashMap<String, String>();
    options.put("changelog-producer", "input");
    options.put("consumer-id", "recovery-reader");
    options.put("consumer.mode", "at-least-once");
    options.put("consumer.expiration-time", "1 d");
    options.put("write-only", Boolean.toString(!mode.equals("first-row")));
    if (mode.equals("sequence")) options.put("sequence.field", "seq,seq2");
    if (mode.equals("first-row"))
      options.putAll(
          Map.of(
              "merge-engine",
              "first-row",
              "ignore-delete",
              "true",
              "changelog-producer",
              "lookup"));
    if (mode.equals("partial-update"))
      options.putAll(Map.of("merge-engine", "partial-update", "ignore-delete", "true"));
    FileStoreTable table = PaimonMergeEngineTest.table(options);
    var read = table.newReadBuilder().withProjection(new int[] {0, 3});
    var scan = read.newStreamScan();
    List<Split> splits;
    try (var writer =
        new PaimonMergeEngineTest.Writer(
            table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
      writer.write(PaimonMergeEngineTest.rows(60, false));
      writer.commit(1);
      writer.write(PaimonMergeEngineTest.rows(90, false));
      writer.commit(2);
      splits = scan.plan().splits();
      if (tail) {
        var changes = PaimonMergeEngineTest.rows(240, false);
        if (mode.equals("first-row")) {
          for (var row : changes)
            ((org.apache.paimon.data.GenericRow) row).setField(0, row.getInt(0) + 100);
        }
        writer.write(changes);
        writer.commit(3);
        splits = scan.plan().splits();
      }
    }
    assertFalse(splits.isEmpty(), "fixture must have records to restore");
    var type = org.apache.paimon.flink.LogicalTypeConversion.toLogicalType(read.readType());
    List<FileStoreSourceSplit> assigned = new ArrayList<>();
    for (int i = 0; i < splits.size(); i++) {
      assigned.add(new FileStoreSourceSplit("split-" + i, splits.get(i)));
    }
    List<String> expected = new ArrayList<>();
    for (Split split : splits) {
      try (var reader = read.newRead().createReader(split)) {
        reader.forEachRemaining(
            row ->
                expected.add(
                    row.getRowKind() + ":" + PaimonTestTables.render(row, read.readType())));
      }
    }
    List<String> actual = new ArrayList<>();
    ReaderOutput<ArrowBatch> output =
        new ReaderOutput<>() {
          @Override
          public void collect(ArrowBatch batch) {
            try (var root = batch.root()) {
              for (var row : RowDataArrowConverter.read(root, type)) {
                actual.add(
                    row.getRowKind()
                        + ":"
                        + PaimonTestTables.render(
                            new org.apache.paimon.flink.FlinkRowWrapper(row), read.readType()));
              }
            }
          }

          @Override
          public void collect(ArrowBatch batch, long timestamp) {
            collect(batch);
          }

          @Override
          public void emitWatermark(Watermark watermark) {}

          @Override
          public void markIdle() {}

          @Override
          public void markActive() {}

          @Override
          public SourceOutput<ArrowBatch> createOutputForSplit(String id) {
            return this;
          }

          @Override
          public void releaseOutputForSplit(String id) {}
        };
    List<FileStoreSourceSplit> saved;
    var source = new NativePaimonSource(table, new int[] {0, 3}, 7, -1);
    try (var reader = source.createReader(new Context())) {
      reader.addSplits(assigned);
      reader.start();
      assertTrue(reader.snapshotState(0).stream().allMatch(s -> s.recordsToSkip() == 0));
      while (actual.isEmpty()) {
        reader.isAvailable().get(10, TimeUnit.SECONDS);
        reader.pollNext(output);
      }
      saved = reader.snapshotState(1);
      assertTrue(actual.size() < expected.size());
      assertEquals(
          actual.size(), saved.stream().mapToLong(FileStoreSourceSplit::recordsToSkip).sum());
      var serializer = source.getSplitSerializer();
      List<FileStoreSourceSplit> restored = new ArrayList<>();
      for (var split : saved) {
        restored.add(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
      }
      saved = restored;
    }
    var restoredSource = new NativePaimonSource(table, new int[] {0, 3}, 3, -1);
    try (var reader = restoredSource.createReader(new Context())) {
      reader.addSplits(saved);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(10, TimeUnit.SECONDS);
        status = reader.pollNext(output);
      } while (status != InputStatus.END_OF_INPUT);
    }
    // Independent bucket splits may reorder on restore; changes of each key must stay ordered.
    java.util.function.Function<String, String> key =
        value -> value.substring(value.indexOf(':') + 1, value.indexOf('|'));
    assertEquals(
        expected.stream().collect(java.util.stream.Collectors.groupingBy(key)),
        actual.stream().collect(java.util.stream.Collectors.groupingBy(key)));
  }

  private static final class Context implements SourceReaderContext {
    private final Configuration config = new Configuration();

    @Override
    public SourceReaderMetricGroup metricGroup() {
      return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
    }

    @Override
    public Configuration getConfiguration() {
      return config;
    }

    @Override
    public String getLocalHostName() {
      return "localhost";
    }

    @Override
    public int getIndexOfSubtask() {
      return 0;
    }

    @Override
    public int currentParallelism() {
      return 1;
    }

    @Override
    public void sendSplitRequest() {}

    @Override
    public void sendSourceEventToCoordinator(SourceEvent event) {}

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
      return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
    }
  }
}
