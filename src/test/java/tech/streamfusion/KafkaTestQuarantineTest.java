package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class KafkaTestQuarantineTest {
  private static final String PIPELINE = "tech.streamfusion.CanonicalKafkaPipelineFailureJobTest";

  @Test
  void matchesOnlyTheArchivedProviderInItsOriginalCohort() {
    assertTrue(
        KafkaTestQuarantine.matches(
            true, false, 17, PIPELINE, "task-failure", false, false, "incrementing"));
    assertFalse(
        KafkaTestQuarantine.matches(
            false, false, 17, PIPELINE, "task-failure", false, false, "incrementing"));
    assertFalse(
        KafkaTestQuarantine.matches(
            true, true, 17, PIPELINE, "task-failure", false, false, "incrementing"));
    assertFalse(
        KafkaTestQuarantine.matches(
            true, false, 11, PIPELINE, "task-failure", false, false, "incrementing"));
    assertFalse(
        KafkaTestQuarantine.matches(
            true, false, 21, PIPELINE, "task-failure", false, false, "incrementing"));
  }

  @Test
  void preservesEveryOtherProviderAndFailureClass() {
    int quarantined = 0;
    for (boolean ordinary : new boolean[] {false, true})
      for (boolean nativeFirst : new boolean[] {false, true})
        for (String naming : new String[] {"incrementing", "pooling"})
          if (KafkaTestQuarantine.matches(
              true, false, 17, PIPELINE, "task-failure", ordinary, nativeFirst, naming))
            quarantined++;
    assertEquals(1, quarantined);
    for (String name : new String[] {
        "tech.streamfusion.CanonicalKafkaSinkFailureJobTest",
        "tech.streamfusion.CanonicalKafkaPipelineProcessFailureJobTest",
        "tech.streamfusion.CanonicalKafkaPipelineCompletionFailureJobTest"})
      assertFalse(
        KafkaTestQuarantine.matches(
            true, false, 17, name, "task-failure", false, false, "incrementing"));
    assertFalse(
        KafkaTestQuarantine.matches(
            true, false, 17, PIPELINE, "process-failure", false, false, "incrementing"));
  }
}
