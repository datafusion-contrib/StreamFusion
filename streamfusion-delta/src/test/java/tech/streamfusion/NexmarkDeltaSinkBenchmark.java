package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Optional-module entry point for the Kafka JSON to Delta merge-on-read Nexmark matrix. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkDeltaSinkBenchmark {

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_DELTA_SINK", matches = "true")
  void mergeOnReadUpsertComparison() throws Exception {
    NexmarkMatrixBenchmark.runDeltaMergeOnReadSinkComparison();
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_DELTA_SINK", matches = "true")
  void mergeOnReadUpsertProfile() throws Exception {
    NexmarkMatrixBenchmark.runDeltaMergeOnReadSinkProfile();
  }
}
