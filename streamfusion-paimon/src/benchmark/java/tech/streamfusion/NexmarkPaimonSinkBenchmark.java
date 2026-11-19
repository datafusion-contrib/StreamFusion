package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Optional-module entry point for the Kafka JSON to Paimon append and primary-key Nexmark matrix. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkPaimonSinkBenchmark {

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_PAIMON_SINK", matches = "true")
  void unawareBucketAppendComparison() throws Exception {
    NexmarkMatrixBenchmark.runPaimonAppendSinkComparison(false);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_PAIMON_SINK", matches = "true")
  void fixedBucketAppendComparison() throws Exception {
    NexmarkMatrixBenchmark.runPaimonAppendSinkComparison(true);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_PAIMON_SINK", matches = "true")
  void primaryKeyComparison() throws Exception {
    NexmarkMatrixBenchmark.runPaimonPrimaryKeySinkComparison();
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_PAIMON_SINK", matches = "true")
  void unawareBucketAppendProfile() throws Exception {
    NexmarkMatrixBenchmark.runPaimonAppendSinkProfile(false);
  }
}
