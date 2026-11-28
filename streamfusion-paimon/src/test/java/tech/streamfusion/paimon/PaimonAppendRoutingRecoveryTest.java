package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.RuntimeCompat;
import tech.streamfusion.planner.NativePlanner;

class PaimonAppendRoutingRecoveryTest {
  private static final int ROWS = 300;
  private static final AtomicBoolean FAILED = new AtomicBoolean();
  private static final AtomicBoolean RESTORED = new AtomicBoolean();

  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(120)
  void localAppendRecoversCommittedRowsLikeStock(boolean partitioned) throws Exception {
    List<List<String>> results = new ArrayList<>();
    for (boolean accelerated : new boolean[] {false, true}) {
      FAILED.set(false);
      RESTORED.set(false);
      Configuration configuration = new Configuration();
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
      var execution = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
      execution.setParallelism(2);
      execution.enableCheckpointing(100);
      var sql = StreamTableEnvironment.create(execution);
      Path warehouse = directory.resolve(accelerated ? "native" : "stock");
      sql.executeSql(
          "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
      sql.useCatalog("p");
      sql.executeSql(
          "CREATE TABLE output_rows (id BIGINT, label STRING, pt STRING)"
              + (partitioned ? " PARTITIONED BY (pt)" : "")
              + " WITH ('bucket'='-1', 'write-only'='true', 'file.format'='parquet')");
      var input =
          execution
              .fromSequence(0, Long.MAX_VALUE)
              .map(new FailAfterCheckpoint())
              .returns(Types.LONG)
              .filter(value -> value < ROWS)
              .map(value -> Row.of(value, value % 7 == 0 ? null : "value" + value, "p" + value % 3))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"id", "label", "pt"}, Types.LONG, Types.STRING, Types.STRING));
      sql.createTemporaryView(
          "input_rows",
          sql.fromDataStream(
              input,
              Schema.newBuilder()
                  .column("id", "BIGINT")
                  .column("label", "STRING")
                  .column("pt", "STRING")
                  .build()));
      var scan = accelerated ? NativePlanner.install(sql) : null;
      var submitted = sql.executeSql("INSERT INTO output_rows SELECT * FROM input_rows");
      FileStoreTable table =
          FileStoreTableFactory.create(
              LocalFileIO.create(),
              new org.apache.paimon.fs.Path(warehouse.resolve("default.db/output_rows").toUri()));
      List<String> committed = Collections.emptyList();
      try {
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (committed.size() != ROWS || !RESTORED.get()) {
          assertTrue(System.nanoTime() < deadline, "all rows must be committed after recovery");
          Thread.sleep(50);
          committed = readRows(table);
        }
        assertTrue(FAILED.get(), "the test must fail after a completed checkpoint");
        if (accelerated) {
          assertTrue(scan.substitutions() > 0, scan.explainSummary());
          assertTrue(
              scan.fallbackReasons().stream()
                  .noneMatch(reason -> reason.startsWith("Paimon sink:")),
              scan.explainSummary());
          assertNativeParquetFiles(table);
        }
      } finally {
        submitted.getJobClient().orElseThrow().cancel().get();
      }
      results.add(committed);
    }
    List<String> expected = new ArrayList<>();
    for (long id = 0; id < ROWS; id++)
      expected.add(id + ":" + (id % 7 == 0 ? null : "value" + id) + ":p" + id % 3);
    Collections.sort(expected);
    assertEquals(expected, results.get(0));
    assertEquals(expected, results.get(1));
  }

  private static List<String> readRows(FileStoreTable table) throws Exception {
    List<String> result = new ArrayList<>();
    var builder = table.newReadBuilder();
    try (var reader = builder.newRead().createReader(builder.newScan().plan())) {
      reader.forEachRemaining(
          row ->
              result.add(
                  row.getLong(0)
                      + ":"
                      + (row.isNullAt(1) ? null : row.getString(1).toString())
                      + ":"
                      + row.getString(2)));
    }
    Collections.sort(result);
    return result;
  }

  private static void assertNativeParquetFiles(FileStoreTable table) throws Exception {
    int files = 0;
    for (var split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit data = (DataSplit) split;
      for (var file : data.dataFiles()) {
        var path = new org.apache.paimon.fs.Path(data.bucketPath(), file.fileName());
        try (var reader = PaimonTestCompat.parquetReader(table.fileIO(), path, file.fileSize())) {
          assertTrue(
              reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"),
              "native writers must encode the recovered files");
        }
        files++;
      }
    }
    assertTrue(files > 0);
  }

  private static final class FailAfterCheckpoint extends RichMapFunction<Long, Long>
      implements CheckpointListener {
    private int seen;

    @Override
    public Long map(Long value) {
      seen++;
      if (RuntimeCompat.attempt(getRuntimeContext()) > 0) RESTORED.set(true);
      LockSupport.parkNanos(2_000_000);
      return value;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      if (seen >= 50 && FAILED.compareAndSet(false, true)) {
        throw new RuntimeException("intentional append failure after checkpoint " + checkpointId);
      }
    }
  }
}
