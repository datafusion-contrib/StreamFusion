package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.Row;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.NativePlanner;

/** End-to-end row-fed writes, and committed reads with production conversion costs retained. */
@EnabledIfEnvironmentVariable(named = "SF_PAIMON1_BENCHMARK", matches = "true")
class LegacyPaimonBenchmark {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir;

  @Test
  void compareStockFallbackAndNative() throws Exception {
    int count = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON1_BENCH_ROWS", "200000"));
    String format = System.getenv().getOrDefault("SF_PAIMON_FILE_FORMAT", "parquet");
    for (boolean primaryKey : List.of(false, true)) {
      FileStoreTable saved = null;
      for (int run = 0; run < 4; run++) {
        for (int offset = 0; offset < 3; offset++) {
          int mode = (run + offset) % 3;
          var warehouse = Files.createTempDirectory(tempDir, "paimon1-benchmark");
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var sql = StreamTableEnvironment.create(env);
          sql.executeSql(
              "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
          sql.executeSql("USE CATALOG p");
          sql.executeSql(
              "CREATE TABLE t (id BIGINT NOT NULL, v BIGINT, label STRING"
                  + (primaryKey ? ", PRIMARY KEY(id) NOT ENFORCED" : "")
                  + ") WITH ('bucket'='"
                  + (primaryKey ? 1 : -1)
                  + "', 'file.format'='"
                  + format
                  + "', 'write-buffer-size'='32 mb', 'write-only'='true')");
          var input =
              env.fromSequence(0, count - 1)
                  .map(i -> Row.of(primaryKey ? i % (count / 10) : i, i, "group-" + i % 100))
                  .returns(
                      Types.ROW_NAMED(
                          new String[] {"id", "v", "label"}, Types.LONG, Types.LONG, Types.STRING));
          sql.createTemporaryView(
              "input_rows",
              sql.fromDataStream(
                  input,
                  Schema.newBuilder()
                      .column("id", DataTypes.BIGINT().notNull())
                      .column("v", DataTypes.BIGINT())
                      .column("label", DataTypes.STRING())
                      .build()));
          String key = "streamfusion.operator.paimonSink.enabled";
          String prior = System.getProperty(key);
          try {
            System.setProperty(key, Boolean.toString(mode == 2));
            if (mode > 0) NativePlanner.install(sql);
            String query = "INSERT INTO t SELECT id, v + 1, label FROM input_rows";
            String plan = sql.explainSql(query);
            assertEquals(mode == 2, plan.contains("NativePaimon"), plan);
            assertEquals(mode > 0, plan.contains("NativeCalc"), plan);
            long start = System.nanoTime();
            sql.executeSql(query).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                "PAIMON1_SINK format=%s pk=%s rows=%d run=%d mode=%d seconds=%.6f%n",
                format, primaryKey, count, run, mode, seconds);
          } finally {
            if (prior == null) System.clearProperty(key);
            else System.setProperty(key, prior);
          }
          var table =
              FileStoreTableFactory.create(
                  LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
          long[] check = new long[2];
          var read = table.newReadBuilder();
          try (var reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(
                r -> {
                  check[0]++;
                  check[1] += r.getLong(1);
                });
          }
          long output = primaryKey ? count / 10 : count;
          assertEquals(output, check[0]);
          assertEquals(output * (2L * count - output + 1) / 2, check[1]);
          saved = table;
        }
      }
      readBenchmark(saved, format, primaryKey);
      if (!primaryKey
          && Boolean.parseBoolean(
              System.getenv().getOrDefault("SF_PAIMON1_SQL_SOURCE_BENCHMARK", "false"))) {
        readSqlBenchmark(saved, format, count);
      }
    }
  }

  private void readSqlBenchmark(FileStoreTable table, String format, int count) throws Exception {
    String warehouse = table.location().getParent().getParent().toString();
    String key = "streamfusion.operator.paimonSource.enabled";
    String prior = System.getProperty(key);
    try {
      for (int run = 0; run < 4; run++) {
        for (int offset = 0; offset < 3; offset++) {
          int mode = (run + offset) % 3;
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var sql = StreamTableEnvironment.create(env);
          sql.getConfig().set("table.exec.mini-batch.enabled", "true");
          sql.getConfig().set("table.exec.mini-batch.size", "4096");
          sql.getConfig().set("table.exec.mini-batch.allow-latency", "10 ms");
          sql.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          sql.executeSql(
              "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse + "')");
          sql.executeSql("USE CATALOG p");
          System.setProperty(key, Boolean.toString(mode == 2));
          if (mode > 0) NativePlanner.install(sql);
          String query = "SELECT COUNT(*), SUM(v), SUM(CHAR_LENGTH(label)) FROM t";
          String plan = sql.explainSql(query);
          assertEquals(mode == 2, plan.contains("NativePaimonSource"), plan);
          assertEquals(mode > 0, plan.contains("NativeColumnarGroupAggregate"), plan);
          var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
          long start = System.nanoTime();
          var result = sql.executeSql(query);
          try (var values = result.collect()) {
            executor
                .submit(
                    () -> {
                      while (values.hasNext()) {
                        Row row = values.next();
                        if ((row.getKind() == org.apache.flink.types.RowKind.INSERT
                                || row.getKind() == org.apache.flink.types.RowKind.UPDATE_AFTER)
                            && ((Number) row.getField(0)).longValue() == count) {
                          assertEquals(
                              (long) count * (count + 1) / 2,
                              ((Number) row.getField(1)).longValue());
                          assertEquals(
                              (long) count * 79 / 10, ((Number) row.getField(2)).longValue());
                          return;
                        }
                      }
                      fail("Snapshot ended without the expected aggregate");
                    })
                .get(45, java.util.concurrent.TimeUnit.SECONDS);
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                "PAIMON1_SQL_SOURCE format=%s rows=%d run=%d mode=%d seconds=%.6f%n",
                format, count, run, mode, seconds);
          } finally {
            result.getJobClient().orElseThrow().cancel().get();
            executor.shutdownNow();
          }
        }
      }
    } finally {
      if (prior == null) System.clearProperty(key);
      else System.setProperty(key, prior);
    }
  }

  private void readBenchmark(FileStoreTable table, String format, boolean primaryKey)
      throws Exception {
    boolean projectValue =
        Boolean.parseBoolean(
            System.getenv().getOrDefault("SF_PAIMON1_SOURCE_PROJECT_VALUE", "false"));
    var read =
        projectValue
            ? table.newReadBuilder().withProjection(new int[] {1})
            : table.newReadBuilder();
    int valueColumn = projectValue ? 0 : 1;
    var type = LogicalTypeConversion.toLogicalType(read.readType());
    var splits = read.newScan().plan().splits();
    Long expected = null;
    for (int run = 0; run < 4; run++) {
      for (int offset = 0; offset < 3; offset++) {
        int mode = (run + offset) % 3;
        long sum = 0;
        long start = System.nanoTime();
        for (var split : splits) {
          if (mode == 2) {
            try (var reader = new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)) {
              reader.handleSplitsChanges(
                  new SplitsAddition<>(List.of(new FileStoreSourceSplit("s", split))));
              boolean finished = false;
              while (!finished) {
                var fetched = reader.fetch();
                if (fetched.nextSplit() != null) {
                  var record = fetched.nextRecordFromSplit();
                  while (record != null) {
                    try (var root = record.batch().root()) {
                      var values =
                          (org.apache.arrow.vector.BigIntVector) root.getVector(valueColumn);
                      for (int i = 0; i < root.getRowCount(); i++) sum += values.get(i);
                    }
                    record = fetched.nextRecordFromSplit();
                  }
                }
                finished = !fetched.finishedSplits().isEmpty();
                fetched.recycle();
              }
              assertTrue(reader.nativeFilesRead() > 0 || reader.nativeSnapshotsRead() > 0);
            }
          } else {
            var serializer = new RowDataSerializer(type);
            List<RowData> pending = new ArrayList<>();
            try (var reader = read.newRead().createReader(split)) {
              var batch = reader.readBatch();
              while (batch != null) {
                var row = batch.next();
                while (row != null) {
                  if (mode == 0) sum += row.getLong(valueColumn);
                  else {
                    pending.add(serializer.copy(new FlinkRowData(row)));
                    if (pending.size() == 4096) {
                      sum += transposeChecksum(pending, type, valueColumn);
                      pending.clear();
                    }
                  }
                  row = batch.next();
                }
                batch.releaseBatch();
                batch = reader.readBatch();
              }
            }
            if (!pending.isEmpty()) sum += transposeChecksum(pending, type, valueColumn);
          }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        if (expected == null) expected = sum;
        else assertEquals(expected.longValue(), sum);
        System.out.printf(
            "PAIMON1_SOURCE format=%s pk=%s run=%d mode=%d seconds=%.6f%n",
            format, primaryKey, run, mode, seconds);
      }
    }
  }

  private long transposeChecksum(
      List<RowData> rows, org.apache.flink.table.types.logical.RowType type, int valueColumn) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, false)) {
      var values = (org.apache.arrow.vector.BigIntVector) root.getVector(valueColumn);
      long sum = 0;
      for (int i = 0; i < root.getRowCount(); i++) sum += values.get(i);
      return sum;
    }
  }
}
