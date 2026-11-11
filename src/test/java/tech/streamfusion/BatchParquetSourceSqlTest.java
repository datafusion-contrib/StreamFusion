package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchParquetSourceSqlTest {
  @TempDir Path directory;
  private static final int ROWS = 2051;

  @Test
  void usesNativeDecodeAndKeepsExplicitOptionAndMetadataFallbacks() throws Exception {
    var file = fixture();
    var config = configuration();
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = environment(false, config);
      stock.executeSql(ddl(file, "", ""));
      String sql = "SELECT k,SUM(v) FROM parquet_input WHERE id>=10 GROUP BY k ORDER BY k";
      var expected = BatchSortSqlTest.collect(stock.executeSql(sql));
      for (String mode : List.of("native", "disabled", "option", "metadata")) {
        var options = new Configuration(config);
        if (mode.equals("disabled"))
          options.setString("streamfusion.operator.fileSource.enabled", "false");
        var env = environment(true, options);
        env.executeSql(
            ddl(
                file,
                mode.equals("metadata")
                    ? ",file_path STRING METADATA FROM 'file.path' VIRTUAL"
                    : "",
                mode.equals("option") ? ",'parquet.batch-size'='128'" : ""));
        var result = env.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(expected, BatchSortSqlTest.collect(result), mode);
        assertTrue(metrics.recordsIn(job, "NativeAggregate") > 0, mode);
        assertEquals(
            mode.equals("native") ? 0 : ROWS,
            metrics.counterValue(job, "numInputRows", "RowDataToArrow"),
            mode);
        assertEquals(
            mode.equals("native") ? ROWS : 0, metrics.counterValue(job, "sourceArrowRows"), mode);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  public static final class FailOnce extends ScalarFunction {
    static final AtomicBoolean FAILED = new AtomicBoolean();

    public Long eval(Long value) {
      if (value != null && value == 1000 && FAILED.compareAndSet(false, true))
        throw new IllegalStateException("retry native Parquet producer");
      return value;
    }
  }

  @Test
  void producerFailureRereadsWholeFileWithoutMissingOrDuplicatingRows() throws Exception {
    var file = fixture();
    var config = configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    config.setString("streamfusion.transpose.batchRows", "127");
    config.setString("streamfusion.native.enabled", "true");
    BatchReplaySinkFactory.FAILED.set(true);
    FailOnce.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var env = NativePlannerTestEnvironment.createBatch(config, BatchReplaySinkFactory.class);
      env.executeSql(ddl(file, "", ""));
      env.executeSql("CREATE TABLE replay(id BIGINT) WITH ('connector'='batch-replay')");
      env.createTemporarySystemFunction("parquet_fail_once", FailOnce.class);
      var result =
          env.executeSql(
              "INSERT INTO replay SELECT id FROM parquet_input WHERE parquet_fail_once(id)>=0 ORDER"
                  + " BY id");
      result.await();
      var completed = result.getJobClient().orElseThrow().getJobExecutionResult().get();
      List<Long> actual = completed.getAccumulatorResult(BatchReplaySinkFactory.RESULTS);
      assertEquals(ROWS, actual.size());
      for (int i = 0; i < ROWS; i++) assertEquals((long) i, actual.get(i));
      assertTrue(FailOnce.FAILED.get());
      assertTrue(
          metrics.attempts("Source: NativeParquetSource").contains("1"),
          metrics.nativeOperators().toString());
      assertTrue(
          metrics.counterValue(
                  result.getJobClient().orElseThrow().getJobID().toString(), "sourceArrowRows")
              >= ROWS);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  private static Configuration configuration() {
    var config = new Configuration();
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("table.optimizer.range-sort.enabled", "false");
    return config;
  }

  private static org.apache.flink.table.api.TableEnvironment environment(
      boolean nativeExecution, Configuration config) throws Exception {
    var options = new Configuration(config);
    options.setString("streamfusion.native.enabled", Boolean.toString(nativeExecution));
    options.setString("parallelism.default", "2");
    return NativePlannerTestEnvironment.createBatch(options);
  }

  private static String ddl(Path file, String columns, String options) {
    return "CREATE TABLE parquet_input(id BIGINT,k INT,v DECIMAL(15,2)"
        + columns
        + ") WITH ('connector'='filesystem','format'='parquet','path'='"
        + file.toUri().toString().replace("'", "''")
        + "'"
        + options
        + ")";
  }

  private Path fixture() throws Exception {
    var type =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new BigIntType(), new IntType(), new DecimalType(15, 2)
            },
            new String[] {"id", "k", "v"});
    var file = directory.resolve("input;part.parquet");
    var path = new org.apache.flink.core.fs.Path(file.toUri());
    try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
      var writer =
          ParquetRowDataBuilder.createWriterFactory(
                  type, new org.apache.hadoop.conf.Configuration(false), false)
              .create(output);
      for (int i = 0; i < ROWS; i++)
        writer.addElement(
            GenericRowData.of(
                (long) i,
                i % 3,
                i % 7 == 0 ? null : DecimalData.fromBigDecimal(new BigDecimal("12.34"), 15, 2)));
      writer.finish();
    }
    return file;
  }
}
