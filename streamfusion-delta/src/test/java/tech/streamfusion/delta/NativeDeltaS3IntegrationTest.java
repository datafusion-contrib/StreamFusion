package tech.streamfusion.delta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.MinIOContainer;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** End-to-end Delta transaction and native data-file write against an S3-compatible store. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeDeltaS3IntegrationTest {

  @Test
  void partitionedDeltaTableCommitsToS3a() throws Exception {
    try (MinIOContainer minio = new MinIOContainer("minio/minio:RELEASE.2024-05-10T01-41-38Z")) {
      minio.start();
      minio.execInContainer("mkdir", "-p", "/data/streamfusion");
      String table = "s3a://streamfusion/delta-table";

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
      DataStream<org.apache.flink.types.Row> source =
          env.fromData(
              Types.ROW_NAMED(
                  new String[] {"id", "payload", "dt"},
                  Types.LONG,
                  Types.ROW_NAMED(
                      new String[] {"name", "numbers"},
                      Types.STRING,
                      Types.OBJECT_ARRAY(Types.INT)),
                  Types.STRING),
              org.apache.flink.types.Row.of(
                  1L, org.apache.flink.types.Row.of("a", new Integer[] {1, 2}), "x"),
              org.apache.flink.types.Row.of(
                  2L, org.apache.flink.types.Row.of("b", new Integer[] {3}), "y"));
      tableEnv.createTemporaryView(
          "changes",
          source,
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column(
                  "payload",
                  DataTypes.ROW(
                      DataTypes.FIELD("name", DataTypes.STRING()),
                      DataTypes.FIELD("numbers", DataTypes.ARRAY(DataTypes.INT()))))
              .column("dt", DataTypes.STRING())
              .build());
      tableEnv.executeSql(
          "CREATE TEMPORARY TABLE sink (id BIGINT, payload ROW<name STRING, numbers ARRAY<INT>>, "
              + "dt STRING) WITH ('connector'='delta', 'table_path'='"
              + table
              + "', 'partitions'='dt', 'fs.s3a.endpoint'='"
              + minio.getS3URL()
              + "', 'fs.s3a.access.key'='"
              + minio.getUserName()
              + "', 'fs.s3a.secret.key'='"
              + minio.getPassword()
              + "', 'fs.s3a.path.style.access'='true', "
              + "'fs.s3a.connection.ssl.enabled'='false', "
              + "'fs.s3a.aws.credentials.provider'="
              + "'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')");
      PhysicalPlanScan scan = NativePlanner.install(tableEnv);

      tableEnv.executeSql("INSERT INTO sink SELECT * FROM changes").await();

      assertTrue(
          scan.substitutions() > 0,
          () -> "Delta S3 sink did not accelerate: " + scan.fallbackReasons());
      Configuration hadoop = new Configuration();
      hadoop.set("fs.s3a.endpoint", minio.getS3URL());
      hadoop.set("fs.s3a.access.key", minio.getUserName());
      hadoop.set("fs.s3a.secret.key", minio.getPassword());
      hadoop.set("fs.s3a.path.style.access", "true");
      hadoop.set("fs.s3a.connection.ssl.enabled", "false");
      hadoop.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
      FileSystem fs = new Path(table).getFileSystem(hadoop);
      assertTrue(fs.exists(new Path(table + "/_delta_log/00000000000000000000.json")));
      StringBuilder files = new StringBuilder();
      RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> listed =
          fs.listFiles(new Path(table), true);
      while (listed.hasNext()) {
        files.append(listed.next().getPath()).append('\n');
      }
      assertTrue(files.toString().contains(".parquet"), () -> "missing Parquet data:\n" + files);

      Engine engine = DefaultEngine.create(hadoop);
      Snapshot snapshot = TableManager.loadSnapshot(table).build(engine);
      Scan deltaScan = snapshot.getScanBuilder().build();
      Set<String> partitions = new HashSet<>();
      try (CloseableIterator<FilteredColumnarBatch> batches = deltaScan.getScanFiles(engine)) {
        while (batches.hasNext()) {
          try (CloseableIterator<Row> rows = batches.next().getRows()) {
            while (rows.hasNext()) {
              AddFile add =
                  new AddFile(
                      rows.next().getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
              partitions.add(add.getPartitionValues().getValues().getString(0));
            }
          }
        }
      }
      assertTrue(partitions.containsAll(Set.of("x", "y")), () -> "partitions=" + partitions);
    }
  }
}
