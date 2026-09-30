package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

class KafkaTopicFixtureTest {
  @Test
  void waitsForAdministrativeMetadataPropagation() throws Exception {
    var attempts = new AtomicInteger();
    KafkaTopicFixture.awaitReady(
        () -> {
          int attempt = attempts.incrementAndGet();
          if (attempt == 1) {
            throw new InvocationTargetException(
                new RuntimeException(new UnknownTopicOrPartitionException("not propagated")));
          }
          if (attempt == 2) throw new LeaderNotAvailableException("electing");
        },
        Duration.ofSeconds(5),
        "test-topic");
    assertEquals(3, attempts.get());
  }

  @Test
  void failsImmediatelyForAuthorizationErrors() {
    var attempts = new AtomicInteger();
    var error = new TopicAuthorizationException("denied");
    assertSame(
        error,
        assertThrows(
            TopicAuthorizationException.class,
            () ->
                KafkaTopicFixture.awaitReady(
                    () -> {
                      attempts.incrementAndGet();
                      throw new InvocationTargetException(error);
                    },
                    Duration.ofSeconds(5),
                    "test-topic")));
    assertEquals(1, attempts.get());
  }

  @Test
  void boundsTheWaitAndRetainsTheCause() {
    var error = new UnknownTopicOrPartitionException("missing");
    var timeout =
        assertThrows(
            TimeoutException.class,
            () ->
                KafkaTopicFixture.awaitReady(
                    () -> {
                      throw error;
                    },
                    Duration.ZERO,
                    "missing-topic"));
    assertSame(error, timeout.getCause());
    assertTrue(timeout.getMessage().contains("missing-topic"));
  }

  @Test
  void preservesInterruptionDuringRetry() {
    try {
      Thread.currentThread().interrupt();
      assertThrows(
          InterruptedException.class,
          () ->
              KafkaTopicFixture.awaitReady(
                  () -> {
                    throw new UnknownTopicOrPartitionException("not propagated");
                  },
                  Duration.ofSeconds(5),
                  "test-topic"));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void preservesInterruption() {
    try {
      assertThrows(
          InterruptedException.class,
          () ->
              KafkaTopicFixture.awaitReady(
                  () -> {
                    throw new InvocationTargetException(new InterruptedException());
                  },
                  Duration.ofSeconds(5),
                  "test-topic"));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
