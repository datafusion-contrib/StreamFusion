package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;

class NativeRocksLifecycleBoundaryTest {
  @TempDir Path directory;

  private long create(String name, String options) {
    return Native.createRocksDBSnapshotStore(
        directory.resolve(name).toString(), 128, options, 0,
        new String[0], new String[0], 0, 127, true);
  }

  @Test
  void constructorErrorsKeepTheirExistingBoundaryClassAndPublishNoHandle() {
    String before = Native.liveNativeHandles();
    assertThrows(NativeMemoryLimitException.class, () -> create("invalid", "not-json"));
    assertEquals(before, Native.liveNativeHandles());
    long handle = create("retry", FlinkRocksDBOptions.from(new Configuration()).json());
    Native.closeRocksDBSnapshotStore(handle);
    assertEquals(before, Native.liveNativeHandles());
  }

  @Test
  void checkpointErrorsKeepTheirRuntimeClassAndTheLiveStoreRemainsUsable() throws Exception {
    long handle = create("live", FlinkRocksDBOptions.from(new Configuration()).json());
    byte[][] partitions = {{0, 0, 0, 0, 1, 2, 3}};
    try {
      Path blocked = directory.resolve("file");
      Files.writeString(blocked, "not a checkpoint directory");
      RuntimeException failure = assertThrows(RuntimeException.class,
          () -> Native.checkpointRocksDBSnapshotStore(handle, partitions, 1234, blocked.toString()));
      assertEquals(RuntimeException.class, failure.getClass());
      assertTrue(failure.getMessage().startsWith("RocksDB checkpoint failed: "));
      String[] manifest = Native.checkpointRocksDBSnapshotStore(
          handle, partitions, 1234, directory.resolve("checkpoint").toString());
      assertTrue(manifest.length > 1);
      assertEquals(1234, Native.rocksdbSnapshotStoreTimerDeadline(handle));
      assertArrayEquals(new byte[] {1, 2, 3}, Native.restoreRocksDBSnapshotStorePartitions(handle)[0]);
    } finally {
      Native.closeRocksDBSnapshotStore(handle);
    }
  }
}
