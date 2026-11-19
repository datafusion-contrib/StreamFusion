package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.NativeMemoryLimitException;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

class TaskOffHeapMemoryTest {

  @RegisterExtension
  final LoggerAuditingExtension logs = new LoggerAuditingExtension(TaskOffHeapMemory.class, Level.WARN);

  private long previousCapacity;

  @BeforeEach
  void savePoolConfiguration() {
    previousCapacity = TaskOffHeapMemory.capacityBytes();
  }

  @AfterEach
  void reset() {
    TaskOffHeapMemory.resetForTests(previousCapacity == 0 ? -1 : previousCapacity);
  }

  @Test
  void aggregatesOwnersAgainstOneProcessCap() {
    TaskOffHeapMemory.resetForTests(100);
    long first = TaskOffHeapMemory.registerOwner("state", "first");
    long second = TaskOffHeapMemory.registerOwner("kafka", "second");

    assertTrue(TaskOffHeapMemory.tryReserve(first, 60));
    assertFalse(TaskOffHeapMemory.tryReserve(second, 41));
    assertTrue(TaskOffHeapMemory.tryReserve(second, 40));
    assertEquals(100, TaskOffHeapMemory.reservedBytes());
    assertEquals(100, TaskOffHeapMemory.peakBytes());

    TaskOffHeapMemory.release(first, 25);
    assertEquals(75, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.closeOwner(first);
    TaskOffHeapMemory.closeOwner(second);
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void spillableStateLeavesHeadroomWithoutPartitioningTheSharedBudget() {
    TaskOffHeapMemory.resetForTests(32);
    long state = TaskOffHeapMemory.registerOwner("state", "sort");
    long arrow = TaskOffHeapMemory.registerOwner("arrow", "next input");
    assertFalse(TaskOffHeapMemory.tryReserveWithHeadroom(state, 17, 16));
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    assertTrue(TaskOffHeapMemory.tryReserveWithHeadroom(state, 16, 16));
    assertTrue(TaskOffHeapMemory.tryReserve(arrow, 16));
    assertEquals(32, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.closeOwner(state);
    TaskOffHeapMemory.closeOwner(arrow);
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void fixedReservationRollsBackAndCloseIsIdempotent() {
    TaskOffHeapMemory.resetForTests(32);
    TaskOffHeapMemory.Reservation reservation =
        TaskOffHeapMemory.reserve("kafka", "queue", 32);
    assertThrows(
        NativeMemoryLimitException.class,
        () -> TaskOffHeapMemory.reserve("kafka", "another", 1));

    reservation.close();
    reservation.close();
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void resettingTestConfigurationCannotHideLiveReservations() {
    TaskOffHeapMemory.resetForTests(32);
    try (TaskOffHeapMemory.Reservation reservation =
        TaskOffHeapMemory.reserve("test", "live reservation", 16)) {
      assertThrows(IllegalStateException.class, () -> TaskOffHeapMemory.resetForTests(64));
      assertEquals(32, TaskOffHeapMemory.capacityBytes());
      assertEquals(16, TaskOffHeapMemory.reservedBytes());
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void denialReportsTheSharedBudgetRatherThanOnlyTheFailingConsumer() {
    TaskOffHeapMemory.resetForTests(100);
    try (TaskOffHeapMemory.Reservation state =
        TaskOffHeapMemory.reserve("operator", "TopN (1/8)", 60)) {
      TaskOffHeapMemory.reserveArrow(35);
      try {
        NativeMemoryLimitException failure =
            assertThrows(NativeMemoryLimitException.class, () -> TaskOffHeapMemory.reserveArrow(8));
        assertTrue(failure.getMessage().contains("shared Arrow allocator"));
        assertTrue(failure.getMessage().contains("8 task off-heap bytes with 5 available"));
        assertTrue(failure.getMessage().contains("capacity=100, reserved=95"));
        assertTrue(failure.getMessage().contains("task/vertex off-heap resource budget"));
        assertEquals(95, TaskOffHeapMemory.reservedBytes());

        TaskOffHeapMemory.releaseArrow(35);
        assertTrue(failure.getMessage().contains("capacity=100, reserved=95"));
      } finally {
        TaskOffHeapMemory.releaseArrow(TaskOffHeapMemory.reservedBytes() - 60);
      }
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void fixedReservationFailureIdentifiesTheOwnerAndRetainsTheOriginalBudget() {
    TaskOffHeapMemory.resetForTests(100);
    try (TaskOffHeapMemory.Reservation first =
        TaskOffHeapMemory.reserve("operator", "TopN (1/8)", 80)) {
      NativeMemoryLimitException failure =
          assertThrows(
              NativeMemoryLimitException.class,
              () -> TaskOffHeapMemory.reserve("connector", "source queue", 21));
      assertTrue(failure.getMessage().contains("connector [source queue]"));
      assertTrue(failure.getMessage().contains("capacity=100, reserved=80"));
      assertEquals(80, TaskOffHeapMemory.reservedBytes());
    }
    try (TaskOffHeapMemory.Reservation retry =
        TaskOffHeapMemory.reserve("operator", "TopN retry", 100)) {
      assertEquals(100, TaskOffHeapMemory.reservedBytes());
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void nativeReservationDenialLogsOwnerIdentityAndPoolSnapshotWithoutThrowing() {
    TaskOffHeapMemory.resetForTests(100);
    long owner = TaskOffHeapMemory.registerOwner("operator", "TopN (3/8)");
    try {
      assertTrue(TaskOffHeapMemory.tryReserve(owner, 80));
      assertTrue(logs.getMessages().isEmpty());
      assertFalse(TaskOffHeapMemory.tryReserve(owner, 21));
      assertEquals(1, logs.getMessages().size());
      String message = logs.getMessages().get(0);
      assertTrue(message.contains("owner " + owner + ":"));
      assertTrue(message.contains("operator [TopN (3/8)]"));
      assertTrue(message.contains("21 task off-heap bytes with 20 available"));
      assertTrue(message.contains("capacity=100, reserved=80"));
      assertTrue(TaskOffHeapMemory.tryReserve(owner, 20));
      assertEquals(100, TaskOffHeapMemory.reservedBytes());
    } finally {
      TaskOffHeapMemory.closeOwner(owner);
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }
}
