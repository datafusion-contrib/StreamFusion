package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.Native;

class NativeRocksSharedResourcesTest {
  @TempDir Path temporary;

  @Test
  void nativeStoresCanInitializeAfterTheBackendReturnsItsPoolLease() {
    try (var pool = new NativeRocksSharedResources(16L << 20, 0.5);
        var first = pool.retain();
        var second = pool.retain()) {
      pool.close();
      pool.close();
      assertThrows(IllegalStateException.class, pool::retain);
      assertStoreCanOpen(first, "first");
      first.close();
      first.close();
      assertThrows(IllegalStateException.class, first::nativeHandle);
      assertStoreCanOpen(second, "second");
    }
  }

  @Test
  void returningOneOperatorLeaseKeepsTheSlotAvailable() {
    try (var pool = new NativeRocksSharedResources(16L << 20, 0.5)) {
      try (var first = pool.retain()) {
        assertStoreCanOpen(first, "first");
      }
      try (var next = pool.retain()) {
        assertStoreCanOpen(next, "next");
      }
    }
  }

  private void assertStoreCanOpen(NativeRocksSharedResources.Lease lease, String name) {
    long store =
        Native.createRocksDBSnapshotStore(
            temporary.resolve(name).toString(),
            1,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            lease.nativeHandle(),
            new String[0],
            new String[0],
            0,
            0,
            false);
    assertNotEquals(0, store);
    try {
      assertEquals(0, Native.restoreRocksDBSnapshotStorePartitions(store).length);
    } finally {
      Native.closeRocksDBSnapshotStore(store);
    }
  }
}
