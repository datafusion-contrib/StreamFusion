package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.MinIOContainer;

/**
 * The native Parquet sink writing to an object store end to end: a partitioned INSERT lands on
 * s3:// through Flink's own s3 filesystem (multipart upload completed on checkpoint commit — the
 * native side only encodes bytes), files and _SUCCESS markers appear under the partition paths, and
 * the rows read back through Flink's own reader.
 *
 * <p>Requires Docker (MinIO); gated like the other container-backed tests.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class FlinkParquetSinkS3IntegrationTest {

  @Test
  void partitionedInsertToS3CommitsAndReadsBack() throws Exception {
    try (MinIOContainer minio = new MinIOContainer("minio/minio:RELEASE.2024-05-10T01-41-38Z")) {
      minio.start();
      // MinIO's filesystem backend serves directories as buckets.
      minio.execInContainer("mkdir", "-p", "/data/streamfusion");

      Configuration s3 = new Configuration();
      s3.setString("s3.endpoint", minio.getS3URL());
      s3.setString("s3.access-key", minio.getUserName());
      s3.setString("s3.secret-key", minio.getPassword());
      s3.setString("s3.path.style.access", "true");
      // ServiceLoader discovery from the test classpath stands in for a deployment's plugins/ dir.
      FileSystem.initialize(s3, null);

      String location = "s3://streamfusion/out";
      PhysicalPlanScan scan = writeInsert(location);
      assertTrue(
          scan.substitutions() > 0,
          () -> "sink did not route to native; reasons=" + scan.fallbackReasons());

      FileSystem fs = new Path(location).getFileSystem();
      assertTrue(
          fs.exists(new Path(location + "/dt=a/_SUCCESS")),
          "partition commit did not write the success marker");
      assertTrue(fs.exists(new Path(location + "/dt=b/_SUCCESS")));

      List<List<Object>> rows = readBack(location);
      assertEquals(3, rows.size(), () -> "rows read back: " + rows);
    }
  }

  private static PhysicalPlanScan writeInsert(String location) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE pq (dt STRING, v INT) PARTITIONED BY (dt) WITH ("
            + "'connector' = 'filesystem', 'path' = '"
            + location
            + "', 'format' = 'parquet',"
            + "'sink.partition-commit.policy.kind' = 'success-file')");
    // Fixed rows through a DataStream view keep the plan a bare source→sink (no Calc to gate on).
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"dt", "v"}, Types.STRING, Types.INT),
            Row.of("a", 1),
            Row.of("a", 2),
            Row.of("b", 3));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder().column("dt", DataTypes.STRING()).column("v", DataTypes.INT()).build());

    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.executeSql("INSERT INTO pq SELECT * FROM src").await();
    return scan;
  }

  private static List<List<Object>> readBack(String location) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE r (dt STRING, v INT) PARTITIONED BY (dt) WITH ("
            + "'connector' = 'filesystem', 'path' = '"
            + location
            + "', 'format' = 'parquet')");
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tEnv.executeSql("SELECT * FROM r").collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(row.getField(i));
        }
        rows.add(fields);
      }
    }
    return rows;
  }
}
