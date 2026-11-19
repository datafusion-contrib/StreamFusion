package tech.streamfusion;

import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.table.HadoopTable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Optional-module entry point for the Kafka JSON to Delta merge-on-read Nexmark matrix. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkDeltaSinkBenchmark {

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_DELTA_SINK", matches = "true")
  void mergeOnReadUpsertComparison() throws Exception {
    NexmarkMatrixBenchmark.runDeltaMergeOnReadSinkComparison(
        NexmarkDeltaSinkBenchmark::createDeletionVectorTable);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_DELTA_SINK", matches = "true")
  void mergeOnReadUpsertProfile() throws Exception {
    NexmarkMatrixBenchmark.runDeltaMergeOnReadSinkProfile(
        NexmarkDeltaSinkBenchmark::createDeletionVectorTable);
  }

  private static void createDeletionVectorTable(
      Path path, org.apache.flink.table.types.logical.RowType rowType) {
    DeltaSinkConf sinkConf = new DeltaSinkConf(rowType, Map.of());
    HadoopTable table =
        new HadoopTable(
            path.toUri(),
            Map.of("delta.enableDeletionVectors", "true"),
            sinkConf.getSinkSchema(),
            List.of());
    table.open();
  }
}
