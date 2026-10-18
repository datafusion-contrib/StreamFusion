package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeIcebergSourceTest {
  @TempDir Path temp;
  private static final ConcurrentSkipListSet<Integer> SEEN = new ConcurrentSkipListSet<>();

  @Test
  void continuousJavaDiscoveryReadsInitialAndLaterSnapshotsAsNativeArrow() throws Exception {
    SEEN.clear();
    var table = NativeIcebergReaderTest.fixture(temp, false);
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "taskmanager.memory.task.off-heap.size",
                "256 mb"));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(2);
    env.enableCheckpointing(100);
    NativeIcebergSource.read(
            env,
            table,
            TableLoader.fromHadoopTable(table.location()),
            config,
            Map.of(
                "streaming",
                "true",
                "monitor-interval",
                "10ms",
                "starting-strategy",
                "TABLE_SCAN_THEN_INCREMENTAL",
                "case-sensitive",
                "true"),
            table.schema(),
            7)
        .map(
            batch -> {
              try (var root = batch.root()) {
                for (int i = 0; i < root.getRowCount(); i++) {
                  int id = (Integer) root.getVector("id").getObject(i);
                  if (!SEEN.add(id)) throw new IllegalStateException("Duplicate Iceberg row " + id);
                }
              }
              return 0;
            })
        .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>());
    var job = env.executeAsync("iceberg-native-continuous-discovery");
    try {
      awaitRows(101);
      var factory =
          new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
      var writer =
          factory.newDataWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/later.parquet")),
              table.spec(),
              null);
      try (writer) {
        var record = GenericRecord.create(table.schema());
        for (int id = 101; id < 104; id++) {
          record.setField("id", id);
          record.setField("payload", "later-" + id);
          writer.write(record);
        }
      }
      table.newAppend().appendFile(writer.toDataFile()).commit();
      awaitRows(104);
      assertEquals(
          java.util.stream.IntStream.range(0, 104)
              .boxed()
              .collect(java.util.stream.Collectors.toSet()),
          SEEN);
    } finally {
      job.cancel().get(30, TimeUnit.SECONDS);
      job.getJobExecutionResult().handle((result, failure) -> null).get(30, TimeUnit.SECONDS);
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static void awaitRows(int count) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (SEEN.size() < count && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(count, SEEN.size());
  }
}
